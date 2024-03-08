use futures::stream::Stream;
use sqlx::{sqlite::SqliteConnectOptions, ConnectOptions, Error as SqlxError, SqliteConnection};
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};
use tokio::sync::mpsc::Receiver;

pub enum StateHandlerMessage {
    StartLogging { container_id: String, fifo: PathBuf },
    StopLogging { fifo: PathBuf },
}

pub struct StateHandler {
    dbcon: SqliteConnection,
}

impl StateHandler {
    pub async fn new(path: &Path) -> Result<Self, SqlxError> {
        let mut dbcon = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
            .create_if_missing(true)
            .connect()
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS active_fetches(container_id text PRIMARY KEY, fifo text)",
        )
        .execute(&mut dbcon)
        .await?;

        Ok(StateHandler { dbcon })
    }

    pub async fn handle(&mut self, mut rx: Receiver<StateHandlerMessage>) -> Result<(), SqlxError> {
        while let Some(msg) = rx.recv().await {
            match msg {
                StateHandlerMessage::StartLogging { container_id, fifo } => {
                    self.start_logging(&container_id, &fifo).await?;
                }
                StateHandlerMessage::StopLogging { fifo } => {
                    self.stop_logging(fifo.as_path().display().to_string().as_str())
                        .await?;
                }
            }
        }

        Ok(())
    }

    async fn start_logging(&mut self, container_id: &str, fifo: &Path) -> Result<(), SqlxError> {
        sqlx::query("INSERT OR REPLACE INTO active_fetches(container_id, fifo) VALUES (?1, ?2)")
            .bind(container_id)
            .bind(fifo.display().to_string())
            .execute(&mut self.dbcon)
            .await?;

        Ok(())
    }

    async fn stop_logging(&mut self, fifo: &str) -> Result<(), SqlxError> {
        sqlx::query("DELETE FROM active_fetches where fifo = ?1")
            .bind(fifo)
            .execute(&mut self.dbcon)
            .await?;

        Ok(())
    }

    pub async fn get_active_fetches<'a>(
        &'a mut self,
    ) -> impl Stream<Item = Result<(String, String), SqlxError>> + 'a {
        sqlx::query_as("SELECT container_id, fifo FROM active_fetches").fetch(&mut self.dbcon)
    }
}
