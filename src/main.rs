mod cleaner;
mod config;
mod docker;
mod logger;

use axum::http::Request;
use axum::middleware::map_request;
use axum::{routing::post, Router, Server};
use docker::ApiState;
use hyperlocal::UnixServerExt;
use log::{self, debug};
use simple_logger;
use std::env;
use std::sync::Arc;
use tokio::{task, time};

async fn normalize_dockerjson<B>(mut req: Request<B>) -> Request<B> {
    let headers = req.headers_mut();
    match headers.get("content-type") {
        Some(ct) => {
            debug!("[normalize_dockerjson] {:?}", ct);
        }
        None => {
            headers.insert("content-type", "application/json".parse().unwrap());
        }
    }

    req
}

#[tokio::main]
async fn main() -> Result<(), config::ParsingError> {
    simple_logger::init_with_level(
        env::var("DEBUG").map_or_else(|_| log::Level::Info, |_| log::Level::Debug),
    )
    .expect("could not set loglevel");

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} [path to config]", args[0]);
        return Ok(());
    }

    let conf: config::Config =
        config::Config::try_from(config::ConfigSource::File(args[1].clone()))?;

    debug!("config: {:?}", &conf);

    let state = Arc::new(ApiState::new(
        conf.databases_dir.to_str().unwrap().to_string(),
        conf.cleanup_age.is_some(),
    ));

    if conf.cleanup_age.is_some() {
        let c = state.cleaner.as_ref().unwrap().clone();
        let c_a = conf.cleanup_age.unwrap();
        task::spawn(async move {
            loop {
                debug!("[cleanup] running...");
                c.cleanup(c_a).ok();
                debug!("[cleanup] sleeping...");
                time::sleep(time::Duration::from_secs(15 * 60)).await;
            }
        });
    }

    let router = Router::new()
        .route("/LogDriver.StartLogging", post(docker::start_logging))
        .route("/LogDriver.StopLogging", post(docker::stop_logging))
        .route("/LogDriver.Capabilities", post(docker::capabilities))
        .route("/LogDriver.ReadLogs", post(docker::read_logs))
        .route("/Plugin.Activate", post(docker::activate))
        .layer(map_request(normalize_dockerjson))
        .fallback(docker::fallback)
        .with_state(state);
    let builder =
        Server::bind_unix(conf.unix_socket_path).expect("could not listen on unix socket");

    builder.serve(router.into_make_service()).await.unwrap();

    Ok(())
}
