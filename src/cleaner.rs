use log::debug;
use sqlx::{Connection, SqliteConnection};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct LogCleaner {
    fifos: Arc<RwLock<HashMap<String, String>>>,
    containers: Arc<RwLock<HashMap<String, usize>>>,
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

    pub async fn add(&self, container_id: &str, fifo: &str) {
        self.fifos
            .write()
            .await
            .insert(fifo.to_string(), container_id.to_string());

        let mut map = self.containers.write().await;

        if let Some(v) = map.get_mut(container_id) {
            *v += 1;
        } else {
            map.insert(container_id.to_string(), 1usize);
        }
    }

    pub async fn remove(&self, fifo: &str) {
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

    pub async fn cleanup(&self, duration: Duration) -> Result<(), sqlx::Error> {
        let mut max_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();

        max_time -= duration;
        for container in self.containers.read().await.keys() {
            debug!(
                "[cleanup] cleaning up container: {} (oldest ts: {}s)",
                container,
                max_time.as_secs()
            );
            let db_url = format!("sqlite://{}/{}", self.dbs_path, container);
            let mut con = SqliteConnection::connect(&db_url).await?;

            sqlx::query("DELETE FROM logs WHERE ts < ?1")
                .bind(max_time.as_secs() as i64)
                .execute(&mut con)
                .await?;
        }

        Ok(())
    }
}
