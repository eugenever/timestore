#![allow(unused)]

use std::sync::Arc;

use arrow::datatypes::{ DataType, Field, Schema };
use arrow::record_batch::{ RecordBatch, RecordBatchIterator };
use axum::{
    response::IntoResponse,
    routing::{ get, post },
    extract::{ State, Json },
    http::StatusCode,
    Router,
};
use axum_streams::*;
use futures::prelude::*;
use tokio::{ task::JoinHandle, time::{ timeout, Duration } };
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;
use tracing::{ event, Level };

use super::error::ServerError;
use super::query_filter::mongo::parse;

use crate::config::Config;
use crate::storage::models::Measurement;
use crate::utils::get_cwd;

#[derive(Clone)]
struct AppState {
    tx_m: flume::Sender<Measurement>,
}

pub async fn run_server(
    config: &Config,
    tx_sm: flume::Sender<Vec<Measurement>>
) -> Result<(), ServerError> {
    let cwd = get_cwd().map_err(ServerError::AnyError)?;
    let addr = format!("{}:{}", config.server.host, config.server.port);
    let listener = tokio::net::TcpListener::bind(&addr).await.map_err(ServerError::IOError)?;

    let (tx_m, rx_m) = flume::unbounded::<Measurement>();
    let _jh_mt = measurement_buffering_task(config, rx_m, tx_sm);

    let state = AppState {
        tx_m,
    };

    let api_routes = Router::new()
        .route("/current_position", get(current_position))
        .route("/measurements", post(create_measurement));
    let app_routes = Router::new()
        .nest("/api", api_routes)
        .nest_service("/", ServeDir::new(format!("{}/public", cwd)));

    let service = app_routes
        .with_state(state)
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    let server = axum::serve(listener, service);
    println!(
        "{} [INFO] Storage server started on {addr}",
        chrono::Local::now().format("%d-%m-%Y %H:%M:%S.%3f")
    );
    server.await.map_err(ServerError::IOError)
}

fn measurement_buffering_task(
    config: &Config,
    rx_m: flume::Receiver<Measurement>,
    tx_sm: flume::Sender<Vec<Measurement>>
) -> Result<JoinHandle<()>, ServerError> {
    let batch_size = config.lancedb.batch_size as usize;
    let timeout_pull = config.lancedb.timeout_pull;
    let mut buffer = Vec::with_capacity(batch_size);

    let jh = tokio::spawn(async move {
        loop {
            // send the buffer of measurements to Storage on timeout or when the threshold value is reached
            match timeout(Duration::from_secs(timeout_pull), rx_m.recv_async()).await {
                Ok(result) => {
                    if let Ok(measurement) = result {
                        if buffer.len() < batch_size {
                            buffer.push(measurement);
                        } else {
                            let batch_vec = std::mem::take(&mut buffer);
                            if let Err(err) = tx_sm.send_async(batch_vec).await {
                                event!(Level::ERROR, "Error send batch of measurements: {}", err);
                            }
                            buffer = Vec::with_capacity(batch_size);
                        }
                    }
                }
                // timeout has occurred, we are forcing a write to the Storage
                Err(_err) => {
                    if buffer.len() > 0 {
                        let batch_vec = std::mem::take(&mut buffer);
                        if let Err(err) = tx_sm.send_async(batch_vec).await {
                            event!(Level::ERROR, "Error send batch of measurements: {}", err);
                        }
                        buffer = Vec::with_capacity(batch_size);
                    }
                }
            }
        }
    });
    Ok(jh)
}

async fn create_measurement(
    State(state): State<AppState>,
    Json(measurement): Json<Measurement>
) -> Result<StatusCode, ServerError> {
    if let Err(err) = state.tx_m.send_async(measurement).await {
        event!(Level::ERROR, "Error create measurements: {}", err);
        return Err(ServerError::AnyError(anyhow::anyhow!("Error create measurements: {}", err)));
    }
    Ok(StatusCode::CREATED)
}

async fn source_current_position_stream() -> Result<impl Stream<Item = RecordBatch>, ServerError> {
    let record_batches: Vec<RecordBatch> = vec![];
    Ok(stream::iter(record_batches))
}

async fn current_position() -> impl IntoResponse {
    let schema = Arc::new(
        Schema::new(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("ts", DataType::Int64, false),
                Field::new("longitude", DataType::Float64, false),
                Field::new("latitude", DataType::Float64, false),
                Field::new("device_id", DataType::Utf8, true),
                Field::new("net_type", DataType::Utf8, true)
            ]
        )
    );

    match source_current_position_stream().await {
        Err(err) => err.into_response(),
        Ok(source_stream) => {
            StreamBodyAs::arrow_ipc(schema.clone(), source_stream).into_response()
        }
    }
}
