mod cleaner;
mod config;
mod docker;
mod logger;
mod statehandler;

use axum::http::Request;
use axum::middleware::map_request;
use axum::{routing::post, Router, Server};
use docker::ApiState;
use hyperlocal::UnixServerExt;
use log::{self, debug, error};
use statehandler::{State, StateHandler};
use std::{env, process::exit, sync::Arc};
use tokio::task;

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

    let conf: Arc<config::Config> = Arc::new(config::Config::try_from(
        config::ConfigSource::File(args[1].clone()),
    )?);

    debug!("config: {:?}", &conf);

    let state = Arc::new(State::new(conf.clone()).await);

    let (mut state_handler, state_handler_tx) = match StateHandler::new(state.clone()).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("failed to initialize StateHandler: {:?}", e);
            exit(1);
        }
    };

    let api_state = Arc::new(
        ApiState::new(state_handler_tx, conf.clone())
            .await
            .expect("Failed to create ApiState"),
    );

    if state.cleaner.is_some() {
        let cleaner = state.cleaner.as_ref().unwrap().clone();
        let cleanup_interval = conf.cleanup_interval.clone();
        task::spawn(async move {
            match cleaner.run(cleanup_interval).await {
                Ok(()) => {
                    error!("Cleaner exited!");
                }
                Err(e) => {
                    error!("Error running cleaner: {:?}", e);
                }
            }
        });
    }

    if let Err(e) = state_handler.replay_state().await {
        eprintln!("Failed to replay state: {:?}", e);
        exit(1);
    }

    let state_handler_handle = tokio::spawn(async move {
        if let Err(e) = state_handler.handle().await {
            error!("Error at StateHandler: {:?}", e);
        }
    });

    let router = Router::new()
        .route("/LogDriver.StartLogging", post(docker::start_logging))
        .route("/LogDriver.StopLogging", post(docker::stop_logging))
        .route("/LogDriver.Capabilities", post(docker::capabilities))
        .route("/LogDriver.ReadLogs", post(docker::read_logs))
        .route("/Plugin.Activate", post(docker::activate))
        .layer(map_request(normalize_dockerjson))
        .fallback(docker::fallback)
        .with_state(api_state);
    let builder = Server::bind_unix(conf.unix_socket_path.as_path())
        .expect("could not listen on unix socket");

    let api_server_handle =
        tokio::spawn(async move { builder.serve(router.into_make_service()).await.unwrap() });

    tokio::select! {
        _ = state_handler_handle => {
            error!("StateHandler exited!");
        }
        _ = api_server_handle => {
            error!("DockerAPI server exited!");
        }
    };

    Ok(())
}
