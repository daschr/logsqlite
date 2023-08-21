mod docker;
mod logger;

use axum::http::Request;
use axum::middleware::map_request;
use axum::{routing::post, Router, Server};
use docker::ApiState;
use hyperlocal::UnixServerExt;
use std::env;
use std::sync::Arc;

const UNIX_SOCKET_PATH: &str = "/run/docker/plugins/logsqlite.sock";

async fn normalize_dockerjson<B>(mut req: Request<B>) -> Request<B> {
    let headers = req.headers_mut();
    match headers.get("content-type") {
        Some(ct) => {
            println!("[normalize_dockerjson] {:?}", ct);
        }
        None => {
            headers.insert("content-type", "application/json".parse().unwrap());
        }
    }

    req
}

#[tokio::main]
async fn main() {
    let state = Arc::new(ApiState::new(
        env::args().nth(1).unwrap_or("./dbs".to_string()),
    ));

    let router = Router::new()
        .route("/LogDriver.StartLogging", post(docker::start_logging))
        .route("/LogDriver.StopLogging", post(docker::stop_logging))
        .route("/LogDriver.Capabilities", post(docker::capabilities))
        .route("/LogDriver.ReadLogs", post(docker::read_logs))
        .route("/Plugin.Activate", post(docker::activate))
        .layer(map_request(normalize_dockerjson))
        .fallback(docker::fallback)
        .with_state(state);
    let builder = Server::bind_unix(UNIX_SOCKET_PATH).expect("could not listen on unix socket");

    builder.serve(router.into_make_service()).await.unwrap();
}
