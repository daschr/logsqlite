use chrono::DateTime;

use log::{debug, error, info};

use core::pin::Pin;
use futures::{
    stream::Stream,
    task::{Context, Poll},
};
use sqlx::{sqlite::SqliteConnectOptions, ConnectOptions, SqliteConnection};
use std::{cell::UnsafeCell, sync::Arc};
use std::{collections::HashMap, str::FromStr};

use prost::Message;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::RwLock;
use tokio::time::timeout;
use tokio::time::{sleep, Duration};
use tokio::{self, task::JoinHandle};

// Include the `items` module, which is generated from items.proto.
pub mod logentry {
    include!(concat!(env!("OUT_DIR"), "/docker.logentry.rs"));
}

pub struct Logger {
    exit: RwLock<bool>,
}

#[derive(Debug)]
pub enum LoggerError {
    IoError(std::io::Error),
    SqlError(sqlx::Error),
    JoinError(tokio::task::JoinError),
    DecodeError(prost::DecodeError),
    EncodeError(prost::EncodeError),
    Exited,
}

impl From<std::io::Error> for LoggerError {
    fn from(e: std::io::Error) -> Self {
        LoggerError::IoError(e)
    }
}

impl From<sqlx::Error> for LoggerError {
    fn from(e: sqlx::Error) -> Self {
        LoggerError::SqlError(e)
    }
}

impl From<tokio::task::JoinError> for LoggerError {
    fn from(e: tokio::task::JoinError) -> Self {
        LoggerError::JoinError(e)
    }
}

impl From<prost::DecodeError> for LoggerError {
    fn from(e: prost::DecodeError) -> Self {
        LoggerError::DecodeError(e)
    }
}

impl From<prost::EncodeError> for LoggerError {
    fn from(e: prost::EncodeError) -> Self {
        LoggerError::EncodeError(e)
    }
}

const ENTRIES_BATCH_SIZE: usize = 10_000;

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
    ) -> Result<Option<u64>, LoggerError> {
        #[allow(unused_assignments)]
        let mut msg_size = 0usize;

        match timeout(std::time::Duration::from_millis(100), reader.read_u32()).await {
            Ok(Ok(v)) => {
                msg_size = v as usize;
            }
            Ok(Err(e)) => {
                // this is some serious error and we cannot continue, the fifo may be closed
                return Err(e.into());
            }
            Err(_) => {
                return Ok(None);
            }
        }

        let mut read = 0;
        let mut bf = [0u8; 1024];
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

        let mut dec_msg = logentry::LogEntry::decode(msg.as_slice())?;
        dec_msg.line.push(b'\n');

        msg.clear();
        msg.extend_from_slice(&(dec_msg.encoded_len() as u32).to_be_bytes());
        msg.reserve(dec_msg.encoded_len());
        dec_msg.encode(msg)?;

        Ok(Some((dec_msg.time_nano as u64) / 1_000_000_000_u64))
    }

    async fn log(&self, fifo: String, db_path: String) -> Result<(), LoggerError> {
        let mut dbcon = SqliteConnectOptions::from_str(&format!("sqlite://{}", &db_path))?
            .create_if_missing(true)
            .connect()
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS logs (ts NUMBER, message BLOB); 
            CREATE INDEX IF NOT EXISTS idx_ts ON logs(ts);",
        )
        .execute(&mut dbcon)
        .await?;

        let mut fd = File::open(fifo.as_str()).await?;
        let mut message: Vec<u8> = Vec::new();

        sqlx::query("BEGIN TRANSACTION;")
            .execute(&mut dbcon)
            .await?;

        let mut n_entries = 0;
        while !*self.exit.read().await {
            debug!("read_protobuf from {}...", fifo.as_str());
            match self.read_protobuf(&mut fd, &mut message).await {
                Ok(Some(ts)) => {
                    sqlx::query("INSERT INTO logs(ts, message) VALUES(?1, ?2)")
                        .bind(ts as i64)
                        .bind(&message)
                        .persistent(true)
                        .execute(&mut dbcon)
                        .await?;

                    n_entries += 1;

                    if n_entries >= ENTRIES_BATCH_SIZE {
                        info!(
                            "reached {} entries, ending current transacion",
                            ENTRIES_BATCH_SIZE
                        );
                        sqlx::query("END TRANSACTION;BEGIN TRANSACTION;")
                            .execute(&mut dbcon)
                            .await?;
                        n_entries = 0;
                    }
                }
                Ok(None) => {
                    info!("read timeout on fifo, ending current transacion");
                    sqlx::query("END TRANSACTION;BEGIN TRANSACTION;")
                        .execute(&mut dbcon)
                        .await?;
                }
                Err(e) => {
                    error!("Failed to read a protobuf message: {:?}", e);
                    return Err(e);
                }
            }
        }

        sqlx::query("END TRANSACTION;").execute(&mut dbcon).await?;

        Err(LoggerError::Exited)
    }

    async fn exit(&self) {
        *self.exit.write().await = true;
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
    pub fn new(dbs_path: String) -> Self {
        LoggerPool {
            dbs_path,
            workers: RwLock::new(HashMap::new()),
        }
    }

    pub async fn start_logging(&self, container_id: &str, fifo_path: &str) {
        let logger = Arc::new(Logger::new());
        let db_path = format!("{}/{}", self.dbs_path, container_id);
        let db_p_c = db_path.clone();
        let f_path = fifo_path.to_string();
        let c_l = logger.clone();
        let handle = tokio::spawn(async move { c_l.log(f_path, db_p_c).await });

        self.workers
            .write()
            .await
            .insert(fifo_path.to_string(), (logger.clone(), handle));
    }

    pub async fn stop_logging(&self, fifo_path: &str) -> Result<(), LoggerError> {
        let res = self.workers.write().await.remove(fifo_path);

        if let Some((logger, handle)) = res {
            logger.exit().await;
            return handle.await?;
        }

        Ok(())
    }
}

// DIRTY WORKAROUND, since we cannot know if the client is disconnected
// so that we can retun Poll::Ready(None) instant of Poll::Pending

const FOLLOW_WAKETIME: u64 = 1;
const FOLLOW_COUNTER_MAX: usize = 60 * 60;

#[derive(Debug)]
pub struct SqliteLogStream {
    stmt_s: String,
    parameters: Vec<u64>,
    next_rowid: u64,
    counter: u64,
    tail: Option<u64>,
    follow: bool,
    follow_counter: usize,
    con: UnsafeCell<SqliteConnection>,
    waker: Option<JoinHandle<()>>,
}

impl SqliteLogStream {
    pub async fn new(
        dbs_path: &str,
        container_id: &str,
        since: Option<String>,
        until: Option<String>,
        tail: Option<u64>,
        follow: bool,
    ) -> Result<Self, sqlx::Error> {
        let db_path = format!("{}/{}", dbs_path, container_id);
        debug!("[SqliteLogStream] db_path: {}", &db_path);
        let mut con = SqliteConnectOptions::from_str(&format!("sqlite://{}", &db_path))?
            .read_only(true)
            .connect()
            .await?;

        let mut cond = String::from("WHERE ROWID >= ?1");
        let mut parameters: Vec<u64> = vec![0];

        if since.is_some() {
            if let Ok(time) = DateTime::parse_from_str(since.as_ref().unwrap().as_str(), "%+") {
                let since = time.timestamp();

                cond.push_str(&format!(" AND ts>=?{}", parameters.len() + 1));
                parameters.push(since as u64);
            }
        };

        if until.is_some() {
            if let Ok(time) = DateTime::parse_from_str(until.as_ref().unwrap().as_str(), "%+") {
                let until = time.timestamp();

                cond.push_str(&format!(" AND ts<=?{}", parameters.len() + 1));
                parameters.push(until as u64);
            }
        };

        let mut first_rowid = 1u64;
        if let Some(tail) = tail {
            let stmt_s = format!("SELECT count(*) FROM logs {}", cond);
            debug!("stmt_s: {} params {:?}", stmt_s, &parameters);

            let mut stmt = sqlx::query_as::<_, (i64,)>(&stmt_s);
            for param in &parameters {
                stmt = stmt.bind(*param as i64);
            }

            let nrows: u64 = stmt.fetch_one(&mut con).await?.0 as u64;

            let stmt_s = format!(
                "SELECT ROWID FROM logs {} LIMIT 1 OFFSET ?{}",
                cond,
                parameters.len() + 1
            );

            debug!("stmt_s: {}", &stmt_s);

            stmt = sqlx::query_as::<_, (i64,)>(&stmt_s);

            parameters.push(if nrows > tail { nrows - tail } else { 0 });

            for param in &parameters {
                stmt = stmt.bind(*param as i64);
            }

            first_rowid = stmt.fetch_one(&mut con).await?.0 as u64;

            parameters.pop();
            debug!("first_rowid: {}", first_rowid);
        }

        let stmt_s = format!("SELECT ROWID,message FROM logs {} LIMIT 1", cond);
        debug!("stmt_s: {} params: {:?}", &stmt_s, &parameters);

        Ok(SqliteLogStream {
            stmt_s,
            parameters,
            next_rowid: first_rowid,
            counter: 0,
            tail: if follow { None } else { tail },
            follow,
            follow_counter: 0,
            con: UnsafeCell::new(con),
            waker: None,
        })
    }
}

impl Drop for SqliteLogStream {
    fn drop(&mut self) {
        if let Some(waker) = self.waker.as_mut() {
            debug!("aborting new_entry_waker");
            waker.abort();
        }
    }
}

impl Stream for SqliteLogStream {
    type Item = Result<Vec<u8>, &'static str>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.tail.is_some() && self.counter >= self.tail.unwrap() {
            return Poll::Ready(None);
        }

        self.parameters[0] = self.next_rowid;
        let res = {
            let stmt_s = self.stmt_s.as_str();
            let mut stmt = sqlx::query_as::<_, (i64, Vec<u8>)>(stmt_s);
            for param in &self.parameters {
                stmt = stmt.bind(*param as i64);
            }

            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    error!("Failed to create single-threaded runtime: {:?}", e);
                    return Poll::Ready(None);
                }
            };

            let res: Option<(i64, Vec<u8>)> = rt
                .block_on(stmt.fetch_one(unsafe { &mut *self.con.get() }))
                .ok();

            /*let res: Option<(u64, Vec<u8>)> = stmt
                            .query_row(rusqlite::params_from_iter(&self.parameters), |r| {
                                let rowid = r.get::<usize, u64>(0)?;
                                let v = r.get::<usize, Vec<u8>>(1)?;
                                Ok((rowid, v))
                            })
                            .ok();
            */
            debug!(
                "[stream] {:?} [{}]",
                res,
                if res.is_some() {
                    res.as_ref().unwrap().1.len()
                } else {
                    0
                }
            );

            res
        };

        if res.is_some() {
            self.counter += 1;
            self.follow_counter = 0;
            self.next_rowid = res.as_ref().unwrap().0 as u64 + 1;
        }

        match res {
            Some((_, r)) => Poll::Ready(Some(Ok(r))),
            None if self.follow && self.follow_counter < FOLLOW_COUNTER_MAX => {
                self.follow_counter += 1;
                let waker = ctx.waker().clone();
                self.waker = Some(tokio::spawn(async move {
                    sleep(Duration::from_secs(FOLLOW_WAKETIME)).await;
                    waker.wake();
                }));

                Poll::Pending
            }
            None => Poll::Ready(None),
        }
    }
}
