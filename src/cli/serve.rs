use tracing::{ event, Level };

use crate::config::load_config;
use crate::log::init_tracing;
use crate::nats::{ client, run_receive_messages_with_nats, queue_subscriber };
use crate::server::error::ServerError;
use crate::server::server::run_server;
use crate::storage::{
    datasets::init_store,
    management::store_management,
    metadata::init_metadata,
    models::Measurement,
    store_directory,
};
use crate::utils::get_cwd;

pub async fn command_serve() -> Result<(), anyhow::Error> {
    let cwd = get_cwd()?;
    let config = load_config(&cwd).await?;

    init_tracing(&config.log_level)?;

    let store_path = store_directory(&cwd, &config);
    init_store(&store_path, &config).await?;

    let metadata_path = format!("{}/metadata", store_path);
    let metadata_tables = init_metadata(&metadata_path).await?;

    // unlimited channel allows you to take all messages from NATS and buffer them inside the service
    let (tx_sm, rx_sm) = flume::unbounded::<Vec<Measurement>>();
    let _handle_sm = store_management(&config, rx_sm, &metadata_tables, &store_path).await?;

    if config.nats.enabled {
        let nats_client = client(&config).await?;
        let nats_subscriber = queue_subscriber(
            &nats_client,
            config.nats.subject.clone(),
            config.nats.queue.clone()
        ).await?;

        let jh_nats = tokio::spawn({
            let config = config.clone();
            let tx_sm = tx_sm.clone();
            async move {
                run_receive_messages_with_nats(nats_subscriber, &config, tx_sm.clone()).await?;
                Ok::<_, anyhow::Error>(())
            }
        });

        let jh_server = tokio::spawn({
            let config = config.clone();
            async move {
                run_server(&config, tx_sm).await?;
                Ok::<_, ServerError>(())
            }
        });

        // Start the http-server and nats at the same time
        let (res_nats, res_server) = tokio::join!(jh_nats, jh_server);

        if let Err(err) = res_nats {
            event!(Level::ERROR, "NATS error: {}", err);
        }
        if let Err(err) = res_server {
            event!(Level::ERROR, "Server error: {:?}", err);
        }
    } else {
        // Start the http-server only
        if let Err(err) = run_server(&config, tx_sm.clone()).await {
            event!(Level::ERROR, "Server error: {:?}", err);
        }
    }

    Ok(())
}
