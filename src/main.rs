mod docker;
mod logger;

use axum::http::Request;
use axum::middleware::map_request;
use axum::{routing::get, routing::post, Router, Server};
//use hyper::server::Server;
use hyperlocal::UnixServerExt;
use std::sync::Arc;
use std::sync::RwLock;

const UNIX_SOCKET_PATH: &str = "/run/docker/plugins/logsqlite.sock";

async fn normalize_dockerjson<B>(mut req: Request<B>) -> Request<B> {
    let mut headers = req.headers_mut();
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
    let state = Arc::new(docker::ApiState::new());

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
