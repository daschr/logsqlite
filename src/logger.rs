use chrono::naive::NaiveDateTime;

use core::pin::Pin;
use futures::{
    stream::Stream,
    task::{Context, Poll},
};
use rusqlite::{Connection, OpenFlags};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::time::timeout;

use tokio;

fn get_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub struct Logger {
    exit: RwLock<bool>,
}

pub enum LoggerError {
    IoError(std::io::Error),
    SqlError(rusqlite::Error),
    Exited,
}

impl From<std::io::Error> for LoggerError {
    fn from(e: std::io::Error) -> Self {
        LoggerError::IoError(e)
    }
}

impl From<rusqlite::Error> for LoggerError {
    fn from(e: rusqlite::Error) -> Self {
        LoggerError::SqlError(e)
    }
}

impl Logger {
    fn new() -> Self {
        Logger {
            exit: RwLock::new(false),
        }
    }

    async fn read_protobuf<R: AsyncRead + Unpin>(
        &self,
        reader: &mut R,
        msg: &mut Vec<u8>,
    ) -> Result<(), LoggerError> {
        #[allow(unused_assignments)]
        let mut msg_size = 0usize;
        loop {
            match timeout(std::time::Duration::from_millis(250), reader.read_u32()).await {
                Ok(Ok(v)) => {
                    msg_size = v as usize;
                    break;
                }
                Ok(Err(_)) => {
                    return Err(LoggerError::Exited);
                }
                Err(_) => {
                    if *self.exit.read().unwrap() {
                        return Err(LoggerError::Exited);
                    }
                }
            }
        }

        let mut read = 0;
        let mut bf = [0u8; 10];
        msg.clear();

        while read < msg_size {
            let tbr = if msg_size - read >= bf.len() {
                bf.len()
            } else {
                msg_size - read
            };
            let read_bytes = reader.read(&mut bf[0..tbr]).await?;

            read += read_bytes;
            msg.extend_from_slice(&bf[0..read_bytes]);
        }

        Ok(())
    }

    async fn log(&self, fifo: String, db_path: String) -> Result<(), LoggerError> {
        let dbcon = Connection::open(db_path)?;
        dbcon.execute(
            "CREATE TABLE IF NOT EXISTS logs (ts NUMBER, message BLOB); 
            CREATE INDEX IF NOT EXISTS idx_ts ON logs(ts);",
            (),
        )?;

        let mut fd = File::open(fifo).await?;

        let mut message: Vec<u8> = Vec::new();

        while !*self.exit.read().unwrap() {
            if let Err(_) = self.read_protobuf(&mut fd, &mut message).await {
                return Ok(());
            }

            dbcon.execute(
                "INSERT INTO logs(ts, message) VALUES(?1, ?2)",
                (get_ts(), &message),
            )?;
        }

        Ok(())
    }

    fn exit(&self) {
        *self.exit.write().unwrap() = true;
    }
}

pub struct LoggerPool {
    pub dbs_path: String,
    workers: RwLock<
        HashMap<
            String,
            (
                Arc<Logger>,
                tokio::task::JoinHandle<Result<(), LoggerError>>,
            ),
        >,
    >,
}

impl LoggerPool {
    pub fn new(dbs_path: &str) -> Self {
        LoggerPool {
            dbs_path: dbs_path.to_string(),
            workers: RwLock::new(HashMap::new()),
        }
    }

    pub fn start_logging(&self, container_id: &str, fifo_path: &str) {
        let logger = Arc::new(Logger::new());
        let db_path = format!("{}/{}", self.dbs_path, container_id);
        let db_p_c = db_path.clone();
        let f_path = fifo_path.to_string();
        let c_l = logger.clone();
        let handle = tokio::spawn(async move { c_l.log(f_path, db_p_c).await });

        self.workers
            .write()
            .unwrap()
            .insert(fifo_path.to_string(), (logger.clone(), handle));
    }

    pub async fn stop_logging(&self, fifo_path: &str) {
        let res = self.workers.write().unwrap().remove(fifo_path);

        if let Some((logger, handle)) = res {
            logger.exit();
            handle.await.ok();
        }
    }
}

#[derive(Debug)]
pub struct SqliteLogStream {
    db_path: String,
    stmt_s: String,
    parameters: Vec<u64>,
    next_rowid: u64,
    counter: usize,
    tail: Option<usize>,
}

impl SqliteLogStream {
    pub fn new(
        dbs_path: &str,
        container_id: &str,
        since: Option<String>,
        tail: Option<usize>,
        follow: bool,
    ) -> Result<Self, rusqlite::Error> {
        let db_path = format!("{}/{}", dbs_path, container_id);
        println!("db_path: {}", &db_path);
        let con = Connection::open_with_flags(
            &db_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;

        let mut cond = String::from("WHERE ROWID >= ?1");
        let mut parameters: Vec<u64> = vec![0];

        if since.is_some() {
            if let Ok(time) =
                NaiveDateTime::parse_from_str(since.as_ref().unwrap().as_str(), "%Y-%m-%dT%H:%M:%S")
            {
                let since = time.timestamp();

                cond.push_str(" AND ts>=?2");
                parameters.push(since as u64);
            }
        };

        let mut first_rowid = 1u64;
        if tail.is_some() {
            let tail = tail.unwrap();
            let mut stmt = con.prepare(&format!("SELECT count(*) FROM logs {}", cond))?;

            let nrows: usize = stmt.query_row(rusqlite::params_from_iter(&parameters), |r| {
                r.get::<usize, usize>(0)
            })?;

            stmt = con.prepare(&format!(
                "SELECT ROWID FROM logs {} LIMIT 1 OFFSET ?1",
                cond
            ))?;

            first_rowid = stmt.query_row([if nrows > tail { nrows - tail } else { 0 }], |r| {
                r.get::<usize, u64>(0)
            })?;

            println!("first_rowid: {}", first_rowid);
        }

        let stmt_s = format!("SELECT message FROM logs {} LIMIT 1", cond);
        println!("stmt_s: {}", &stmt_s);

        Ok(SqliteLogStream {
            db_path,
            stmt_s,
            parameters,
            next_rowid: first_rowid,
            counter: 0,
            tail: if follow { None } else { tail },
        })
    }
}

impl<'a> Stream for SqliteLogStream {
    type Item = Vec<u8>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.tail.is_some() && self.counter >= self.tail.unwrap() {
            return Poll::Ready(None);
        }

        self.parameters[0] = self.next_rowid;

        let con = match Connection::open_with_flags(
            &self.db_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        ) {
            Ok(c) => c,
            Err(_) => return Poll::Ready(None),
        };

        let mut stmt = match con.prepare(&self.stmt_s) {
            Ok(s) => s,
            Err(_) => return Poll::Ready(None),
        };

        let res: Option<Vec<u8>> = stmt
            .query_row(rusqlite::params_from_iter(&self.parameters), |r| {
                r.get::<usize, Vec<u8>>(0)
            })
            .ok();

        println!(
            "res: {:?} [{}]",
            &res,
            if res.is_some() {
                res.as_ref().unwrap().len()
            } else {
                0
            }
        );
        if res.is_some() {
            self.counter += 1;
        }

        self.next_rowid += 1;

        Poll::Ready(res)
    }
}
