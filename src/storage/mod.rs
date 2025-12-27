pub mod conversion;
pub mod datasets;
pub mod management;
pub mod metadata;
pub mod models;
pub mod schema;

use std::sync::Arc;

use arrow_array::{ ArrayRef, RecordBatch, RecordBatchIterator };
use arrow_schema::Schema;
use lance::dataset::{ WriteMode, WriteParams };
use lancedb::table::{ AddDataMode, CompactionOptions, OptimizeAction, WriteOptions };
use lancedb::{ Table, connect };
use tokio::task::JoinHandle;

use crate::config::Config;

pub fn store_directory(cwd: &str, config: &Config) -> String {
    let config_store_path = &config.lancedb.store_path;
    if config_store_path.contains("{cwd}") {
        config_store_path.replace("{cwd}", cwd)
    } else {
        config_store_path.clone()
    }
}

pub async fn save_batches(
    table: &Table,
    schema: Arc<Schema>,
    columns: Vec<ArrayRef>
) -> Result<(), anyhow::Error> {
    let batches = RecordBatchIterator::new(
        vec![RecordBatch::try_new(schema.clone(), columns)?].into_iter().map(Ok),
        schema.clone()
    );
    let write_params = WriteParams {
        mode: WriteMode::Append,
        ..Default::default()
    };

    table.checkout_latest().await?;
    table
        .add(batches)
        .mode(AddDataMode::Append)
        .write_options(WriteOptions {
            lance_write_params: Some(write_params),
        })
        .execute().await?;

    table.optimize(OptimizeAction::Compact {
        options: CompactionOptions {
            target_rows_per_fragment: 2 * 1024 * 1024,
            batch_size: Some(5 * 1024 * 1024),
            materialize_deletions_threshold: 0.1,
            ..Default::default()
        },
        remap_options: None,
    }).await?;

    table.optimize(OptimizeAction::Prune {
        older_than: chrono::TimeDelta::try_seconds(1),
        delete_unverified: None,
        error_if_tagged_old_versions: None,
    }).await?;

    Ok(())
}

/*
    dataset:
    - "{cwd}/store/points"
    - "{cwd}/store/measuremenets/GSM/250/99/654/345356"
*/
pub async fn compact_table(
    dataset: &str,
    table_partition_prefix: &str,
    id: u32
) -> Result<(), anyhow::Error> {
    // TODO: remove after tests
    let st = std::time::Instant::now();

    let partition = format!("{}_{}", table_partition_prefix, id);
    let conn = connect(dataset).execute().await?;
    let table = conn.open_table(partition).execute().await?;
    table.checkout_latest().await?;

    table.optimize(OptimizeAction::Compact {
        options: CompactionOptions {
            target_rows_per_fragment: 2 * 1024 * 1024,
            batch_size: Some(5 * 1024 * 1024),
            materialize_deletions_threshold: 0.1,
            ..Default::default()
        },
        remap_options: None,
    }).await?;

    table.optimize(OptimizeAction::Prune {
        older_than: chrono::TimeDelta::try_seconds(1),
        delete_unverified: None,
        error_if_tagged_old_versions: None,
    }).await?;

    // TODO: remove after tests
    println!("COMPACT {dataset} duration: {:?}", st.elapsed());

    Ok(())
}

fn spawn_compact_table(
    dataset: &str,
    table_partition_prefix: &str,
    id: u32
) -> JoinHandle<Result<(), anyhow::Error>> {
    tokio::spawn({
        let dataset = dataset.to_string();
        let table_partition_prefix = table_partition_prefix.to_string();
        async move {
            compact_table(&dataset, &table_partition_prefix, id).await?;
            Ok(())
        }
    })
}
