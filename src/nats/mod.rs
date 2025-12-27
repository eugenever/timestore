use async_nats::{ Client, ConnectOptions, Subscriber };
use futures::stream::StreamExt;
use tokio::time::{ timeout, Duration };
use tracing::{ event, Level };

use crate::config::Config;
use crate::storage::models::Measurement;

pub async fn client(config: &Config) -> Result<Client, anyhow::Error> {
    let client = ConnectOptions::new()
        .subscription_capacity(config.nats.subscription_capacity)
        .connect(&config.nats.url).await?;
    Ok(client)
}

#[allow(unused)]
pub async fn subscriber(client: &Client, subject: String) -> Result<Subscriber, anyhow::Error> {
    let s = client.subscribe(subject).await?;
    Ok(s)
}

pub async fn queue_subscriber(
    client: &Client,
    subject: String,
    queue: String
) -> Result<Subscriber, anyhow::Error> {
    let s = client.queue_subscribe(subject, queue).await?;
    Ok(s)
}

pub async fn run_receive_messages_with_nats(
    mut subscriber: Subscriber,
    config: &Config,
    tx_sm: flume::Sender<Vec<Measurement>>
) -> Result<(), anyhow::Error> {
    let batch_size = config.lancedb.batch_size as usize;
    let timeout_pull = config.lancedb.timeout_pull;
    let mut buffer = Vec::with_capacity(batch_size);

    loop {
        // send the buffer of messages to Storage on timeout or when the threshold value is reached
        match timeout(Duration::from_secs(timeout_pull), subscriber.next()).await {
            Ok(result) => {
                if let Some(message) = result {
                    if
                        let Ok(measurement) = serde_json::from_slice::<Measurement>(
                            &message.payload
                        )
                    {
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
}
