use bincode::{deserialize, serialize};
use futures_util::StreamExt;
use sqlx::Sqlite;
use sqlx::{sqlite::SqliteConnectOptions, ConnectOptions, Error as SqlxError, SqliteConnection};
use std::fs;
use std::sync::Arc;
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::cleaner::LogCleaner;
use crate::config::{Config, LogConfig};
use crate::log::{info, warn};
use crate::logger::{LoggerError, LoggerPool};

pub struct State {
    logger_pool: LoggerPool,
    config: Arc<Config>,
    pub cleaner: LogCleaner,
}

impl State {
    pub async fn new(config: Arc<Config>) -> Self {
        State {
            logger_pool: LoggerPool::new(config.clone()),
            cleaner: LogCleaner::new(config.databases_dir.display().to_string()),
            config,
        }
    }

    async fn start_logging(
        &self,
        container_id: &str,
        fifo: &Path,
        log_conf: LogConfig,
        tx: Sender<StateHandlerMessage>,
    ) {
        self.cleaner
            .add(&container_id, fifo.to_path_buf(), log_conf.clone())
            .await;

        self.logger_pool
            .start_logging(&container_id, fifo.to_path_buf(), log_conf, tx)
            .await;
    }
}

pub enum StateHandlerMessage {
    StartLogging {
        container_id: String,
        fifo: PathBuf,
        log_conf: LogConfig,
    },
    StopLogging {
        fifo: PathBuf,
    },
    LoggingStopped {
        container_id: String,
        fifo: PathBuf,
        result: Result<(), LoggerError>,
    },
}

pub struct StateHandler {
    dbcon: SqliteConnection,
    rx: Receiver<StateHandlerMessage>,
    tx: Sender<StateHandlerMessage>,
    state: Arc<State>,
}

impl StateHandler {
    pub async fn new(state: Arc<State>) -> Result<(Self, Sender<StateHandlerMessage>), SqlxError> {
        let mut dbcon = SqliteConnectOptions::from_str(&format!(
            "sqlite://{}",
            state.config.state_database.display()
        ))?
        .create_if_missing(true)
        .connect()
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS active_fetches(container_id text PRIMARY KEY, fifo text, log_conf blob)",
        )
        .execute(&mut dbcon)
        .await?;

        let (tx, rx) = channel(1024);

        Ok((
            StateHandler {
                dbcon,
                rx,
                tx: tx.clone(),
                state,
            },
            tx,
        ))
    }

    pub async fn handle(&mut self) -> Result<(), SqlxError> {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                StateHandlerMessage::StartLogging {
                    container_id,
                    fifo,
                    log_conf,
                } => {
                    info!("[StateHandler] starting logging of {}", container_id);

                    let blob_log_conf: Vec<u8> = serialize(&log_conf).unwrap();
                    sqlx::query(
                        "INSERT OR REPLACE INTO active_fetches(container_id, fifo, log_conf) VALUES (?1, ?2, ?3)",
                    )
                    .bind(&container_id)
                    .bind(fifo.display().to_string())
                    .bind(blob_log_conf)
                    .execute(&mut self.dbcon)
                    .await?;

                    self.state
                        .start_logging(&container_id, &fifo, log_conf, self.tx.clone())
                        .await;
                }
                StateHandlerMessage::StopLogging { fifo } => {
                    info!("[StateHandler] stopping logging of fifo {}", fifo.display());

                    sqlx::query("DELETE FROM active_fetches where fifo = ?1")
                        .bind(fifo.as_path().display().to_string())
                        .execute(&mut self.dbcon)
                        .await?;

                    self.state.logger_pool.stop_logging(&fifo).await;
                }
                StateHandlerMessage::LoggingStopped {
                    container_id,
                    fifo,
                    result,
                } => {
                    if let Err(e) = result {
                        warn!(
                            "[StateHandler] logger of container {} returned: {:?}",
                            container_id, e
                        );
                        match e {
                            LoggerError::DecodeError(_) => {
                                info!(
                                "[StateHandler] restarting logging of {} using fifo {}, since last error was a decode error",
                                &container_id,
                                fifo.display()
                                );

                                if let Some(log_conf) = self.state.cleaner.remove(&fifo).await {
                                    self.state
                                        .start_logging(
                                            &container_id,
                                            &fifo,
                                            log_conf,
                                            self.tx.clone(),
                                        )
                                        .await;
                                } else {
                                    warn!("Container {} stopped but it had no LogConfig in the cleaner!", container_id);
                                }
                            }
                            _ => {
                                sqlx::query("DELETE FROM active_fetches where fifo = ?1")
                                    .bind(fifo.as_path().display().to_string())
                                    .execute(&mut self.dbcon)
                                    .await?;

                                if let Some(log_conf) = self.state.cleaner.remove(&fifo).await {
                                    info!("log_conf: '{:?}'", &log_conf);
                                    if log_conf.delete_when_stopped {
                                        let mut dbpath = self.state.config.databases_dir.clone();
                                        dbpath.push(container_id);

                                        info!("removing {}", dbpath.display());
                                        fs::remove_file(dbpath).ok();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn replay_state(&mut self) -> Result<(), sqlx::Error> {
        let mut stream = sqlx::query_as::<Sqlite, (String, String, Vec<u8>)>(
            "SELECT container_id, fifo, log_conf FROM active_fetches",
        )
        .fetch(&mut (self.dbcon));

        while let Some(r) = stream.next().await {
            let (container_id, fifo, blob_log_conf) = r?;

            let log_conf: LogConfig = deserialize(&blob_log_conf).unwrap();
            info!(
                "[StateHandler] replaying logging of {} using fifo {}",
                container_id, fifo
            );

            self.state
                .start_logging(
                    &container_id,
                    PathBuf::from(fifo).as_path(),
                    log_conf,
                    self.tx.clone(),
                )
                .await;
        }

        Ok(())
    }
}
