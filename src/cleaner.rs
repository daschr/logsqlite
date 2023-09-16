use log::debug;
use rusqlite::{Connection, OpenFlags};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

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

    pub fn add(&self, container_id: &str, fifo: &str) {
        self.fifos
            .write()
            .unwrap()
            .insert(fifo.to_string(), container_id.to_string());

        let mut map = self.containers.write().unwrap();

        if let Some(v) = map.get_mut(container_id) {
            *v += 1;
        } else {
            map.insert(container_id.to_string(), 1usize);
        }
    }

    pub fn remove(&self, fifo: &str) {
        let container_id: String = match self.fifos.read().unwrap().get(fifo).cloned() {
            Some(v) => v,
            None => return,
        };

        let mut map = self.containers.write().unwrap();

        if let Some(v) = map.get_mut(&container_id) {
            if *v > 1 {
                *v -= 1;
                return;
            }
        }

        map.remove(&container_id);
    }

    pub fn cleanup(&self, duration: Duration) -> Result<(), rusqlite::Error> {
        let mut max_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();

        max_time -= duration;
        for container in self.containers.read().unwrap().keys() {
            debug!(
                "[cleanup] cleaning up container: {} (oldest ts: {}s)",
                container,
                max_time.as_secs()
            );
            let db_path = format!("{}/{}", self.dbs_path, container);
            let con = Connection::open_with_flags(
                &db_path,
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_URI,
            )?;

            con.execute("DELETE FROM logs WHERE ts < ?1", [max_time.as_secs()])?;
        }

        Ok(())
    }
}
