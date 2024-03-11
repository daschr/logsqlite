use futures_util::StreamExt;
use sqlx::Sqlite;
use sqlx::{sqlite::SqliteConnectOptions, ConnectOptions, Error as SqlxError, SqliteConnection};

use std::sync::Arc;
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::cleaner::LogCleaner;
use crate::config::Config;
use crate::log::{error, info};
use crate::logger::{LoggerError, LoggerPool};

pub struct State {
    logger_pool: LoggerPool,
    config: Arc<Config>,
    pub cleaner: Option<LogCleaner>,
}

impl State {
    pub async fn new(config: Arc<Config>) -> Self {
        State {
            logger_pool: LoggerPool::new(config.clone()),
            cleaner: if config.cleanup_age.is_some() || config.cleanup_max_lines.is_some() {
                Some(LogCleaner::new(
                    config.databases_dir.display().to_string(),
                    config.cleanup_age,
                    config.cleanup_max_lines,
                ))
            } else {
                None
            },
            config,
        }
    }

    async fn start_logging(
        &self,
        container_id: &str,
        fifo: &Path,
        tx: Sender<StateHandlerMessage>,
    ) {
        if self.cleaner.is_some() {
            self.cleaner
                .as_ref()
                .unwrap()
                .add(&container_id, fifo.to_path_buf())
                .await;
        }

        self.logger_pool
            .start_logging(&container_id, fifo.to_path_buf(), tx)
            .await;
    }

    async fn stop_logging(&self, fifo: &Path) {
        if self.cleaner.is_some() {
            self.cleaner.as_ref().unwrap().remove(fifo).await;
        }

        self.logger_pool.stop_logging(fifo).await;
    }
}

pub enum StateHandlerMessage {
    StartLogging {
        container_id: String,
        fifo: PathBuf,
    },
    StopLogging {
        fifo: PathBuf,
    },
    LoggingStopped {
        container_id: String,
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
            "CREATE TABLE IF NOT EXISTS active_fetches(container_id text PRIMARY KEY, fifo text)",
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
                StateHandlerMessage::StartLogging { container_id, fifo } => {
                    info!("[StateHandler] starting logging of {}", container_id);

                    sqlx::query(
                        "INSERT OR REPLACE INTO active_fetches(container_id, fifo) VALUES (?1, ?2)",
                    )
                    .bind(&container_id)
                    .bind(fifo.display().to_string())
                    .execute(&mut self.dbcon)
                    .await?;

                    self.state
                        .start_logging(&container_id, &fifo, self.tx.clone())
                        .await;
                }
                StateHandlerMessage::StopLogging { fifo } => {
                    info!("[StateHandler] stopping logging of fifo {}", fifo.display());

                    let fifo_str = fifo.as_path().display().to_string();
                    sqlx::query("DELETE FROM active_fetches where fifo = ?1")
                        .bind(&fifo_str)
                        .execute(&mut self.dbcon)
                        .await?;

                    self.state.stop_logging(&fifo).await;
                }
                StateHandlerMessage::LoggingStopped {
                    container_id,
                    result,
                } => {
                    if let Err(e) = result {
                        error!(
                            "[StateHandler] logger of container {} returned: {:?}",
                            container_id, e
                        );
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn replay_state(&mut self) -> Result<(), sqlx::Error> {
        let mut stream = sqlx::query_as::<Sqlite, (String, String)>(
            "SELECT container_id, fifo FROM active_fetches",
        )
        .fetch(&mut (self.dbcon));

        while let Some(r) = stream.next().await {
            let (container_id, fifo) = r?;

            info!(
                "[StateHandler] replaying logigng of {} using fifo {}",
                container_id, fifo
            );

            self.state
                .start_logging(
                    &container_id,
                    PathBuf::from(fifo).as_path(),
                    self.tx.clone(),
                )
                .await;
        }

        Ok(())
    }
}
