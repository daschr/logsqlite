use log::{debug, error, info};
use sqlx::{Connection, SqliteConnection};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::{sync::RwLock, time};

#[derive(Clone)]
pub struct LogCleaner {
    fifos: Arc<RwLock<HashMap<PathBuf, String>>>,
    containers: Arc<RwLock<HashMap<String, usize>>>,
    dbs_path: String,
    cleanup_age: Option<Duration>,
    cleanup_max_lines: Option<u64>,
}

impl LogCleaner {
    pub fn new(
        dbs_path: String,
        cleanup_age: Option<Duration>,
        cleanup_max_lines: Option<u64>,
    ) -> Self {
        if cleanup_age.is_none() && cleanup_max_lines.is_none() {
            panic!("cleanup_age and cleanup_max_lines cannot be both None!");
        }

        LogCleaner {
            fifos: Arc::new(RwLock::new(HashMap::new())),
            containers: Arc::new(RwLock::new(HashMap::new())),
            dbs_path,
            cleanup_age,
            cleanup_max_lines,
        }
    }

    pub async fn add(&self, container_id: &str, fifo: PathBuf) {
        self.fifos
            .write()
            .await
            .insert(fifo, container_id.to_string());

        let mut map = self.containers.write().await;

        if let Some(v) = map.get_mut(container_id) {
            *v += 1;
        } else {
            map.insert(container_id.to_string(), 1usize);
        }
    }

    pub async fn remove(&self, fifo: &Path) {
        let container_id: String = match self.fifos.read().await.get(fifo).cloned() {
            Some(v) => v,
            None => return,
        };

        let mut map = self.containers.write().await;

        if let Some(v) = map.get_mut(&container_id) {
            if *v > 1 {
                *v -= 1;
                return;
            }
        }

        map.remove(&container_id);
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

    async fn cleanup_db(&self, con: &mut SqliteConnection) -> Result<(), sqlx::Error> {
        match (self.cleanup_age, self.cleanup_max_lines) {
            (Some(cleanup_age), Some(max_lines)) => {
                let rowid = Self::get_first_tail_rowid(con, max_lines as u64).await?;

                let mut max_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap();

                max_time -= cleanup_age;

                debug!(
                    "cleanup: DELETE FROM logs WHERE ts < {} OR ROWID < {}",
                    max_time.as_secs(),
                    rowid
                );
                sqlx::query("DELETE FROM logs WHERE ts < ?1 OR ROWID < ?2")
                    .bind(max_time.as_secs() as i64)
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
                    max_time.as_secs()
                );
                sqlx::query("DELETE FROM logs WHERE ts < ?1")
                    .bind(max_time.as_secs() as i64)
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
            for container in self.containers.read().await.keys() {
                debug!(
                    "[cleanup] cleaning up container: {}, max_age: {:?} max_lines: {:?}",
                    container, self.cleanup_age, self.cleanup_max_lines
                );
                let db_url = format!("sqlite://{}/{}", self.dbs_path, container);
                match SqliteConnection::connect(&db_url).await {
                    Err(e) => {
                        error!("[cleanup] failed to open connection: {:?}", e);
                    }
                    Ok(mut con) => {
                        if let Err(e) = self.cleanup_db(&mut con).await {
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
