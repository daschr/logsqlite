use crate::config::{Config, LogConfig};
use crate::logger::SqliteLogStream;
use crate::statehandler::StateHandlerMessage;

use axum::{
    body::StreamBody,
    debug_handler,
    extract::{Json, State},
    http::Uri,
    response::IntoResponse,
};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::mpsc::Sender;

pub struct ApiState {
    pub state_handler_tx: Sender<StateHandlerMessage>,
    pub config: Arc<Config>,
}

impl ApiState {
    pub async fn new(
        state_handler_tx: Sender<StateHandlerMessage>,
        config: Arc<Config>,
    ) -> Result<Self, sqlx::Error> {
        Ok(ApiState {
            state_handler_tx,
            config,
        })
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
    info!("[start_logging] conf: {:?}", conf);

    state
        .state_handler_tx
        .send(StateHandlerMessage::StartLogging {
            container_id: conf.Info.ContainerID.clone(),
            fifo: PathBuf::from(conf.File.as_str()),
            log_conf: match LogConfig::try_from(&conf.Info.Config) {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to parse config: {:?}: {:?}", conf.Info.Config, e);
                    return json!({"Err": e}).into();
                }
            },
        })
        .await
        .expect("failed to enqueue StateHandlerMessage");

    info!("[docker::start_logging] send StartLogging message to StateHandler");

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
    info!("[stop_logging] conf: {:?}", conf);

    state
        .state_handler_tx
        .send(StateHandlerMessage::StopLogging {
            fifo: PathBuf::from(conf.File),
        })
        .await
        .expect("Failed to enqueue StateHandlerMessage");
    info!("[docker::stop_logging] send StopLogging message to StateHandler");

    json!({"Err": ""}).into()
}

pub async fn capabilities() -> Json<Value> {
    debug!("[capabilities] called");
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

/*
Object {"Config": Object {"Follow": Bool(false), "Since": String("0001-01-01T00:00:00Z"),
"Tail": Number(-1), "Until": String("0001-01-01T00:00:00Z")},
"Info": Object {"Config": Object {}, "ContainerArgs": Array [], "ContainerCreated": String("2023-08-17T14:15:45.983205858Z"), "ContainerEntrypoint": String("bash"), "ContainerEnv": Array [String("PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")], "ContainerID": String("71a33f78bc9b362c91de1746d54d8fea51a17bfddde1745c2f9fbfdb1e4d2b90"), "ContainerImageID": String("sha256:01f29b872827fa6f9aed0ea0b2ede53aea4ad9d66c7920e81a8db6d1fd9ab7f9"), "ContainerImageName": String("ubuntu"), "ContainerLabels": Object {"org.opencontainers.image.ref.name": String("ubuntu"), "org.opencontainers.image.version": String("22.04")}, "ContainerName": String("/wonderful_raman"), "DaemonName": String("docker"), "LogPath": String("")}}
*/

pub async fn read_logs(
    State(state): State<Arc<ApiState>>,
    Json(mut conf): Json<ReadLogsConf>,
) -> Result<impl IntoResponse, Json<Value>> {
    info!("[read_logs] conf: {:?}", conf);

    let tail = match conf.Config.Tail {
        Some(v) if v < 1 => None,
        Some(v) => Some(v as u64),
        None => None,
    };

    if conf
        .Config
        .Since
        .as_mut()
        .is_some_and(|x| x == "0001-01-01T00:00:00Z")
    {
        conf.Config.Since = None;
    }

    if conf
        .Config
        .Until
        .as_mut()
        .is_some_and(|x| x == "0001-01-01T00:00:00Z")
    {
        conf.Config.Until = None;
    }

    let logstream = match SqliteLogStream::new(
        &state.config.databases_dir.as_path(),
        &conf.Info.ContainerID,
        conf.Config.Since,
        conf.Config.Until,
        tail,
        conf.Config.Follow.unwrap_or(false),
    )
    .await
    {
        Ok(l) => l,
        Err(e) => {
            error!("Error creating logstream: {:?}", &e);
            return Err(
                json!({ "Err": format!("[logsqlite] Could not read logs: {:?}", e) }).into(),
            );
        }
    };

    Ok(StreamBody::new(logstream))
}

pub async fn activate() -> Json<Value> {
    debug!("activate called");
    json!({"Implements": ["LogDriver"]}).into()
}

pub async fn fallback(uri: Uri) -> &'static str {
    debug!("[fallback] uri: {:?}", uri);
    "not found"
}
