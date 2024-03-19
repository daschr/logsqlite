use log::{debug, error, info};
use sqlx::{Connection, SqliteConnection};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::{sync::RwLock, time};

use crate::config::LogConfig;

#[derive(Clone)]
pub struct LogCleaner {
    fifos: Arc<RwLock<HashMap<PathBuf, String>>>,
    containers: Arc<RwLock<HashMap<String, LogConfig>>>,
    dbs_path: String,
}

impl LogCleaner {
    pub fn new(dbs_path: String) -> Self {
        LogCleaner {
            fifos: Arc::new(RwLock::new(HashMap::new())),
            containers: Arc::new(RwLock::new(HashMap::new())),
            dbs_path,
        }
    }

    pub async fn add(&self, container_id: &str, fifo: PathBuf, log_conf: LogConfig) {
        self.fifos
            .write()
            .await
            .insert(fifo, container_id.to_string());

        let mut map = self.containers.write().await;
        map.insert(container_id.to_string(), log_conf);
    }

    pub async fn remove(&self, fifo: &Path) -> Option<LogConfig> {
        let container_id: String = match self.fifos.write().await.remove(fifo) {
            Some(v) => v,
            None => return None,
        };

        self.containers.write().await.remove(&container_id)
    }

    async fn get_first_tail_rowid(
        con: &mut SqliteConnection,
        tail: u64,
    ) -> Result<u64, sqlx::Error> {
        let count = match sqlx::query_as::<sqlx::Sqlite, (i64,)>("SELECT count(*) FROM logs")
            .fetch_one(&mut *con)
            .await
        {
            Ok(v) => v.0 as u64,
            Err(sqlx::Error::RowNotFound) => {
                return Ok(0);
            }
            Err(e) => {
                return Err(e);
            }
        };

        let rowid =
            sqlx::query_as::<sqlx::Sqlite, (i64,)>("SELECT ROWID FROM logs LIMIT 1 OFFSET ?1")
                .bind(if count > tail {
                    (count - tail) as i64
                } else {
                    0
                })
                .fetch_one(&mut *con)
                .await?
                .0 as u64;

        Ok(rowid)
    }

    async fn cleanup_db(
        &self,
        log_conf: &LogConfig,
        con: &mut SqliteConnection,
    ) -> Result<(), sqlx::Error> {
        match (log_conf.cleanup_age, log_conf.cleanup_max_lines) {
            (Some(cleanup_age), Some(max_lines)) => {
                let rowid = Self::get_first_tail_rowid(con, max_lines as u64).await?;

                let mut max_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap();

                max_time -= cleanup_age;

                debug!(
                    "cleanup: DELETE FROM logs WHERE ts < {} OR ROWID < {}",
                    max_time.as_nanos(),
                    rowid
                );
                sqlx::query("DELETE FROM logs WHERE ts < ?1 OR ROWID < ?2")
                    .bind(max_time.as_nanos() as i64)
                    .bind(rowid as i64)
                    .execute(con)
                    .await?;
            }
            (Some(cleanup_age), None) => {
                let mut max_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap();

                max_time -= cleanup_age;

                debug!(
                    "cleanup: DELETE FROM logs WHERE ts < {}",
                    max_time.as_nanos()
                );
                sqlx::query("DELETE FROM logs WHERE ts < ?1")
                    .bind(max_time.as_nanos() as i64)
                    .execute(con)
                    .await?;
            }
            (None, Some(max_lines)) => {
                let rowid = Self::get_first_tail_rowid(con, max_lines as u64).await?;

                debug!("cleanup: DELETE FROM logs WHERE ROWID < {}", rowid);
                sqlx::query("DELETE FROM logs WHERE ROWID < ?1")
                    .bind(rowid as i64)
                    .execute(con)
                    .await?;
            }
            (None, None) => (), // never happens
        }

        Ok(())
    }

    pub async fn run(&self, cleanup_interval: Duration) -> Result<(), sqlx::Error> {
        loop {
            info!("starting cleanup");
            for (container, log_conf) in self.containers.read().await.iter() {
                debug!(
                    "[cleanup] cleaning up container: {}, max_age: {:?} max_lines: {:?}",
                    container, log_conf.cleanup_age, log_conf.cleanup_max_lines
                );
                let db_url = format!("sqlite://{}/{}", self.dbs_path, container);
                match SqliteConnection::connect(&db_url).await {
                    Err(e) => {
                        error!("[cleanup] failed to open connection: {:?}", e);
                    }
                    Ok(mut con) => {
                        if let Err(e) = self.cleanup_db(&log_conf, &mut con).await {
                            error!("[cleanup] could not cleanup {}: {:?}", container, e);
                        }
                    }
                }
            }
            info!("cleanup done");

            time::sleep(cleanup_interval).await;
        }
    }
}
