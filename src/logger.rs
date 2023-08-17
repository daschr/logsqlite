use chrono::naive::NaiveDateTime;

use core::pin::Pin;
use futures::{
    future::join,
    stream::Stream,
    task::{Context, Poll},
};
use rusqlite::{params, Connection, OpenFlags, Rows, Statement, ToSql};
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
            match timeout(std::time::Duration::from_secs(3), reader.read_u32()).await {
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
        let mut bf = [0u8; 256];
        msg.clear();

        while read < msg_size {
            let tbr = if msg_size - read >= bf.len() {
                bf.len()
            } else {
                msg_size - read
            };
            let read_bytes = reader.read(&mut bf[0..tbr]).await?;
            read += read_bytes;
            msg.extend_from_slice(&bf[0..tbr]);
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
    dbs_path: String,
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

struct SqliteLogStream<'a> {
    db_path: String,
    parameters: Vec<Box<dyn ToSql>>,
    cur_rowid: u64,
    last_rowid: Option<u64>,
}

impl<'a> SqliteLogStream<'a> {
    fn new(
        dbs_path: &str,
        container_id: &str,
        since: Option<&str>,
        tail: Option<usize>,
        follow: bool,
    ) -> Result<Self, rusqlite::Error> {
        let db_path = format!("{}/{}", dbs_path, container_id);
        let con = Arc::new(Box::new(Connection::open_with_flags(
            &db_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?));

        let mut cur_rowid: u64 = 0;
        let mut cond = String::from("WHERE ROWID >= ?1");
        let mut parameters: Vec<Box<dyn ToSql>> = vec![cur_rowid];

        if since.is_some() {
            if let Ok(time) = NaiveDateTime::parse_from_str(since.unwrap(), "%Y-%m-%dT%H:%M:%S") {
                let since = time.timestamp();

                cond.push_str(" AND ts>=?2");
                parameters.push(Box::new(since));
            }
        };

        if tail.is_some() {
            let tail = tail.unwrap();
            let mut stmt = con.prepare(&format!("SELECT count(*) FROM logs {}", cond))?;

            let nrows: usize = stmt.query_row(rusqlite::params_from_iter(&parameters), |r| {
                r.get::<usize, usize>(1)
            })?;

            cond.push_str(&format!(
                " LIMIT {} OFFSET {}",
                tail,
                if nrows > tail { nrows - tail } else { 0 }
            ));
        }

        let stmt_s = format!("SELECT message FROM LOGS {}", cond);
        println!("stmt_s: {}", &stmt_s);

        let mut stmt = Arc::new(Box::new(con.clone().prepare(stmt_s.as_str())?));
        let rows = Arc::new(Box::new(
            stmt.clone()
                .query(rusqlite::params_from_iter(&parameters))?,
        ));

        Ok(SqliteLogStream {
            db_path,
            con: con,
            stmt: stmt,
            rows: rows,
            intitial_query_finished: false,
            last_rowid: None,
            follow,
        })
    }
}

impl<'a> Stream for SqliteLogStream<'a> {
    type Item = Vec<u8>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}
