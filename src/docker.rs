use crate::logger::LoggerPool;

use axum::debug_handler;
use axum::extract::{Json, State};
use axum::http::Uri;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

pub struct ApiState {
    logger_pool: LoggerPool,
}

impl ApiState {
    pub fn new() -> Self {
        ApiState {
            logger_pool: LoggerPool::new("./dbs"),
        }
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub struct Info {
    Config: Option<HashMap<String, String>>,
    ContainerID: String,
    ContainerName: Option<String>,
    ContainerEntrypoint: Option<String>,
    ContainerArgs: Option<Vec<String>>,
    ContainerImageID: Option<String>,
    ContainerImageName: Option<String>,
    ContainerCreated: Option<String>,
    ContainerEnv: Option<Vec<String>>,
    ContainerLabels: Option<HashMap<String, String>>,
    LogPath: Option<String>,
    DaemonName: Option<String>,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub struct StartLoggingConf {
    File: String,
    Info: Info,
}

pub async fn start_logging(
    State(state): State<Arc<ApiState>>,
    Json(conf): Json<StartLoggingConf>,
) -> Json<Value> {
    println!("[start_logging] conf: {:?}", conf);

    state
        .logger_pool
        .start_logging(&conf.Info.ContainerID, &conf.File);

    json!({"Err": ""}).into()
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub struct StopLoggingConf {
    File: String,
}

#[debug_handler]
pub async fn stop_logging(
    State(state): State<Arc<ApiState>>,
    Json(conf): Json<StopLoggingConf>,
) -> Json<Value> {
    println!("[stop_logging] conf: {:?}", conf);
    state.logger_pool.stop_logging(&conf.File).await;
    println!("stopped");
    json!({"Err": ""}).into()
}

pub async fn capabilities() -> Json<Value> {
    json!({"ReadLogs": true}).into()
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ReadConfig {
    Since: Option<String>,
    Tail: Option<usize>,
    Follow: Option<bool>,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ReadLogsConf {
    ReadConfig: ReadConfig,
    Info: Info,
}

pub async fn read_logs(
    State(state): State<Arc<ApiState>>,
    Json(conf): Json<ReadLogsConf>,
) -> Json<Value> {
    println!("[read_logs] conf: {:?}", conf);
    json!({"Err": "not implemented"}).into()
}

pub async fn activate() -> Json<Value> {
    println!("activate called");
    json!({"Implements": ["LogDriver"]}).into()
}

pub async fn fallback(uri: Uri) -> &'static str {
    println!("[fallback] uri: {:?}", uri);
    "not found"
}
