use crate::logger::LoggerPool;
use crate::logger::SqliteLogStream;

use axum::debug_handler;
use axum::extract::{Json, State};
use axum::http::Uri;
use axum::response::IntoResponse;

use axum_streams::*;
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
    println!("[capabilities] called");
    json!({"Cap": {"ReadLogs": true}}).into()
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ReadConfig {
    Since: Option<String>,
    Until: Option<String>,
    Tail: Option<i128>,
    Follow: Option<bool>,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ReadLogsConf {
    Config: ReadConfig,
    Info: Info,
}

/* Object {"Config": Object {"Follow": Bool(false), "Since": String("0001-01-01T00:00:00Z"),
"Tail": Number(-1), "Until": String("0001-01-01T00:00:00Z")},
"Info": Object {"Config": Object {}, "ContainerArgs": Array [], "ContainerCreated": String("2023-08-17T14:15:45.983205858Z"), "ContainerEntrypoint": String("bash"), "ContainerEnv": Array [String("PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")], "ContainerID": String("71a33f78bc9b362c91de1746d54d8fea51a17bfddde1745c2f9fbfdb1e4d2b90"), "ContainerImageID": String("sha256:01f29b872827fa6f9aed0ea0b2ede53aea4ad9d66c7920e81a8db6d1fd9ab7f9"), "ContainerImageName": String("ubuntu"), "ContainerLabels": Object {"org.opencontainers.image.ref.name": String("ubuntu"), "org.opencontainers.image.version": String("22.04")}, "ContainerName": String("/wonderful_raman"), "DaemonName": String("docker"), "LogPath": String("")}}

*/
pub async fn read_logs(
    State(state): State<Arc<ApiState>>,
    Json(conf): Json<ReadLogsConf>,
) -> Result<impl IntoResponse, Json<Value>> {
    println!("[read_logs] conf: {:?}", conf);

    let tail = match conf.Config.Tail {
        Some(v) if v < 1 => None,
        Some(v) => Some(v as usize),
        None => None,
    };

    let logstream = match SqliteLogStream::new(
        &state.logger_pool.dbs_path,
        &conf.Info.ContainerID,
        conf.Config.Since,
        tail,
        conf.Config.Follow.unwrap_or(false),
    ) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Error creating logstream: {:?}", &e);
            return Err(
                json!({ "Err": format!("[logsqlite] Could not read logs: {:?}", e) }).into(),
            );
        }
    };

    Ok(StreamBodyAs::protobuf(logstream))
}

pub async fn activate() -> Json<Value> {
    println!("activate called");
    json!({"Implements": ["LogDriver"]}).into()
}

pub async fn fallback(uri: Uri) -> &'static str {
    println!("[fallback] uri: {:?}", uri);
    "not found"
}
