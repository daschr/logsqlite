use crate::config::{Config, LogConfig};
use chrono::DateTime;
use log::{debug, error, info, warn};

use crate::statehandler::StateHandlerMessage;
use tokio::sync::mpsc::Sender;

use core::pin::Pin;
use futures::{
    executor::block_on,
    stream::Stream,
    task::{Context, Poll},
};
use sqlx::{sqlite::SqliteConnectOptions, ConnectOptions, SqliteConnection};
use std::path::Path;
use std::{cell::UnsafeCell, collections::HashMap, path::PathBuf, str::FromStr, sync::Arc};

use prost::Message;
use tokio::{
    self,
    fs::File,
    io::{AsyncRead, AsyncReadExt},
    sync::RwLock,
    task::JoinHandle,
    time::{sleep, timeout, Duration, Instant},
};

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
        read_timeout: Duration,
    ) -> Result<Option<u64>, LoggerError> {
        #[allow(unused_assignments)]
        let mut msg_size = 0usize;

        match timeout(read_timeout, reader.read_u32()).await {
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

        Ok(Some(dec_msg.time_nano as u64))
    }

    async fn log(
        &self,
        fifo: PathBuf,
        db_path: PathBuf,
        log_conf: LogConfig,
    ) -> Result<(), LoggerError> {
        let mut dbcon =
            SqliteConnectOptions::from_str(&format!("sqlite://{}", &db_path.display()))?
                .create_if_missing(true)
                .connect()
                .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS logs (ts NUMBER, message BLOB); 
            CREATE INDEX IF NOT EXISTS idx_ts ON logs(ts);",
        )
        .execute(&mut dbcon)
        .await?;

        let mut fd = File::open(&fifo).await?;
        let mut message: Vec<u8> = Vec::new();

        sqlx::query("BEGIN TRANSACTION;")
            .execute(&mut dbcon)
            .await?;

        let mut no_entries_ts = Instant::now();
        let mut nb_entries = 0;
        let mut acc_entries_size = 0;

        while !*self.exit.read().await {
            debug!("read_protobuf from {}...", fifo.display());
            match self
                .read_protobuf(&mut fd, &mut message, log_conf.message_read_timeout)
                .await
            {
                Ok(Some(ts)) => {
                    sqlx::query("INSERT INTO logs(ts, message) VALUES(?1, ?2)")
                        .bind(ts as i64)
                        .bind(&message)
                        .persistent(true)
                        .execute(&mut dbcon)
                        .await?;

                    nb_entries += 1;
                    acc_entries_size += message.len();

                    if nb_entries >= log_conf.max_lines_per_tx
                        || acc_entries_size >= log_conf.max_size_per_tx
                    {
                        sqlx::query("END TRANSACTION;BEGIN TRANSACTION;")
                            .execute(&mut dbcon)
                            .await?;

                        info!(
                            "reached {} entries with {} bytes size, ending current transacion ({:.2} lines/s) for {}",
                            nb_entries,
                            acc_entries_size,
                            nb_entries as f64
                                * (1_000_000f64
                                    / Instant::now().duration_since(no_entries_ts).as_micros()
                                        as f64),
                            fifo.display()
                        );

                        no_entries_ts = Instant::now();
                        nb_entries = 0;
                        acc_entries_size = 0;
                    }
                }
                Ok(None) => {
                    sqlx::query("END TRANSACTION;BEGIN TRANSACTION;")
                        .execute(&mut dbcon)
                        .await?;

                    no_entries_ts = Instant::now();
                    nb_entries = 0;
                    acc_entries_size = 0;
                }
                Err(e) => {
                    warn!("Failed to read a protobuf message: {:?}", e);
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
    workers: RwLock<HashMap<PathBuf, (Arc<Logger>, tokio::task::JoinHandle<()>)>>,
    config: Arc<Config>,
}

impl LoggerPool {
    pub fn new(config: Arc<Config>) -> Self {
        LoggerPool {
            workers: RwLock::new(HashMap::new()),
            config,
        }
    }

    pub async fn start_logging(
        &self,
        container_id: &str,
        fifo_path: PathBuf,
        log_conf: LogConfig,
        tx: Sender<StateHandlerMessage>,
    ) {
        let logger = Arc::new(Logger::new());
        let mut db_path = self.config.databases_dir.clone();
        db_path.push(container_id);

        let f_path = fifo_path.clone();
        let c_l = logger.clone();

        let c_container_id = container_id.to_string();

        let handle = tokio::spawn(async move {
            tx.send(StateHandlerMessage::LoggingStopped {
                container_id: c_container_id,
                fifo: f_path.clone(),
                result: c_l.log(f_path, db_path, log_conf).await,
            })
            .await
            .expect("Could not enqueue StateHandlerMessage");
        });

        self.workers
            .write()
            .await
            .insert(fifo_path, (logger.clone(), handle));
    }

    pub async fn stop_logging(&self, fifo_path: &Path) {
        let res = self.workers.write().await.remove(fifo_path);

        if let Some((logger, handle)) = res {
            logger.exit().await;
            handle.await.ok();
        }
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
        dbs_path: &Path,
        container_id: &str,
        since: Option<String>,
        until: Option<String>,
        tail: Option<u64>,
        follow: bool,
    ) -> Result<Self, sqlx::Error> {
        let db_path = format!("{}/{}", dbs_path.display(), container_id);
        debug!("[SqliteLogStream] db_path: {}", &db_path);
        let mut con = SqliteConnectOptions::from_str(&format!("sqlite://{}", &db_path))?
            .immutable(true)
            .read_only(true)
            .serialized(false)
            .connect()
            .await?;

        let mut cond = String::from("WHERE ROWID >= ?1");
        let mut parameters: Vec<u64> = vec![0];

        if since.is_some() {
            if let Ok(time) = DateTime::parse_from_str(since.as_ref().unwrap().as_str(), "%+") {
                let since = time
                    .timestamp_nanos_opt()
                    .expect("timestamp in nanosecs does not fit in i64 anymore");

                cond.push_str(&format!(" AND ts>=?{}", parameters.len() + 1));
                parameters.push(since as u64);
            }
        };

        if until.is_some() {
            if let Ok(time) = DateTime::parse_from_str(until.as_ref().unwrap().as_str(), "%+") {
                let until = time
                    .timestamp_nanos_opt()
                    .expect("timestamp in nanosecs does not fit in i64 anymore");

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

            let res: Option<(i64, Vec<u8>)> =
                match block_on(stmt.fetch_one(unsafe { &mut *self.con.get() })) {
                    Ok(s) => Some(s),
                    Err(sqlx::Error::RowNotFound) => None,
                    Err(e) => {
                        error!("[SqliteLogStream] Got SQL error: {:?}", e);
                        return Poll::Ready(None);
                    }
                };

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

impl Drop for SqliteLogStream {
    fn drop(&mut self) {
        if let Some(waker) = self.waker.as_mut() {
            debug!("aborting new_entry_waker");
            waker.abort();
        }
    }
}
