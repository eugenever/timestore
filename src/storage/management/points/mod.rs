pub mod utils;

use std::sync::Arc;

use arrow_array::{
    ArrayRef,
    RecordBatch,
    RecordBatchIterator,
    StringArray,
    UInt32Array,
    UInt64Array,
};
use arrow_schema::Schema;
use futures::StreamExt;
use lance::{ dataset::{ WriteMode, WriteParams }, Dataset };
use lancedb::table::{ AddDataMode, CompactionOptions, OptimizeAction, WriteOptions };
use lancedb::{ connect, Table };
use itertools::izip;

#[derive(Debug, Default, Clone)]
pub struct StatisticsPartitionPoints {
    pub id: Option<u32>,
    pub partition: String,
    pub start: Option<u64>,
    pub end: Option<u64>,
    pub count_rows: u64,
    pub _remote_store: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct StatisticPoints {
    pub statistics: Vec<StatisticsPartitionPoints>,
}

// creating the first partial point table and adding it to the metadata
pub async fn init_points_table(
    store_path: &str,
    table_partition_prefix: &str,
    points_schema: Arc<Schema>,
    points_metadata_schema: Arc<Schema>,
    points_metadata_table: &Table
) -> Result<(), anyhow::Error> {
    let conn = connect(&format!("{store_path}/points")).execute().await?;
    let tables = conn.table_names().execute().await?;
    // if the storage does not have tables, create the first partial table
    if tables.len() == 0 {
        let partition = format!("{}_1", table_partition_prefix);
        conn.create_empty_table(&partition, points_schema).execute().await?;

        // current timestamp minus 1 year (seconds)
        let now_ts = (chrono::offset::Utc::now().timestamp() as u64) - 31_536_000;
        let remote_store: Vec<Option<String>> = vec![None];
        // Add meta info
        let columns: Vec<ArrayRef> = vec![
            // id = 1
            Arc::new(UInt32Array::from(vec![1])),
            // partition = partition_1
            Arc::new(StringArray::from(vec![partition])),
            // start = now timestamp in seconds
            Arc::new(UInt64Array::from(vec![Some(now_ts)])),
            // end = Null
            Arc::new(UInt64Array::from(vec![None])),
            // count_rows = 0
            Arc::new(UInt64Array::from(vec![0])),
            // remote_store
            Arc::new(StringArray::from(remote_store))
        ];

        add_partition_table_to_metadata_points(
            points_metadata_table,
            points_metadata_schema,
            columns
        ).await?;
    }
    Ok(())
}

// creating new partial point table and adding it to the metadata
pub async fn create_partition_points_table(
    store_path: &str,
    table_partition_prefix: &str,
    partition_id: u32,
    points_schema: Arc<Schema>,
    points_metadata_schema: Arc<Schema>,
    points_metadata_table: &Table,
    start: u64
) -> Result<Table, anyhow::Error> {
    let conn = connect(&format!("{store_path}/points")).execute().await?;
    let partition = format!("{}_{}", table_partition_prefix, partition_id);
    let table = conn.create_empty_table(&partition, points_schema).execute().await?;
    // Add meta info
    let remote_store: Vec<Option<String>> = vec![None];
    let columns: Vec<ArrayRef> = vec![
        // id = partition_id
        Arc::new(UInt32Array::from(vec![partition_id])),
        // partition = partition_i
        Arc::new(StringArray::from(vec![partition])),
        // start = input timestamp
        Arc::new(UInt64Array::from(vec![start])),
        // end = Null
        Arc::new(UInt64Array::from(vec![None])),
        // count_rows = 0
        Arc::new(UInt64Array::from(vec![0])),
        // remote_store
        Arc::new(StringArray::from(remote_store))
    ];
    add_partition_table_to_metadata_points(
        points_metadata_table,
        points_metadata_schema,
        columns
    ).await?;
    Ok(table)
}

// load all partial points tables into memory
pub async fn load_points_metadata_table(
    points_metadata_table: &Table
) -> Result<StatisticPoints, anyhow::Error> {
    let uri = points_metadata_table.dataset_uri();
    let ds = Dataset::open(uri).await?;
    let mut scanner = ds.scan();
    scanner.project(&["id", "partition", "start", "end", "count_rows", "remote_store"])?;

    let capacity = points_metadata_table.count_rows(None).await?;
    let mut statistics: Vec<StatisticsPartitionPoints> = Vec::with_capacity(capacity);

    let mut batch_stream = scanner.try_into_stream().await?.map(|b| b.unwrap());
    while let Some(batch) = batch_stream.next().await {
        let array_id = batch
            .column_by_name("id")
            .ok_or(anyhow::anyhow!("Error get 'id' arrow array from RecordBatch"))?
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or(anyhow::anyhow!("Error downcast 'id' arrow array to UInt32Array"))?
            .into_iter()
            .map(|id| id)
            .collect::<Vec<_>>();
        let array_partition = batch
            .column_by_name("partition")
            .ok_or(anyhow::anyhow!("Error get 'partition' arrow array from RecordBatch"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(anyhow::anyhow!("Error downcast 'partition' arrow array to StringArray"))?
            .into_iter()
            .map(|p| p.unwrap().to_string())
            .collect::<Vec<_>>();
        let array_start = batch
            .column_by_name("start")
            .ok_or(anyhow::anyhow!("Error get 'start' arrow array from RecordBatch"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow::anyhow!("Error downcast 'start' arrow array to UInt64Array"))?
            .into_iter()
            .map(|s| s)
            .collect::<Vec<_>>();
        let array_end = batch
            .column_by_name("end")
            .ok_or(anyhow::anyhow!("Error get 'end' arrow array from RecordBatch"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow::anyhow!("Error downcast 'end' arrow array to UInt64Array"))?
            .into_iter()
            .map(|e| e)
            .collect::<Vec<_>>();
        let array_cr = batch
            .column_by_name("count_rows")
            .ok_or(anyhow::anyhow!("Error get 'count_rows' arrow array from RecordBatch"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow::anyhow!("Error downcast 'count_rows' arrow array to UInt64Array"))?
            .into_iter()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>();
        let array_rs = batch
            .column_by_name("remote_store")
            .ok_or(anyhow::anyhow!("Error get 'remote_store' arrow array from RecordBatch"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(anyhow::anyhow!("Error downcast 'remote_store' arrow array to StringArray"))?
            .into_iter()
            .map(|rs| rs.map(|v| v.to_string()))
            .collect::<Vec<_>>();

        let mut pt: Vec<StatisticsPartitionPoints> = Vec::with_capacity(array_id.len());
        for (id, partition, start, end, count_rows, _remote_store) in izip!(
            array_id,
            array_partition,
            array_start,
            array_end,
            array_cr,
            array_rs
        ) {
            pt.push(StatisticsPartitionPoints {
                id,
                partition,
                start,
                end,
                count_rows,
                _remote_store,
            });
        }

        statistics.append(&mut pt);
    }

    // sort by partition id,
    // so that the last element of the array always represents the current partial table
    statistics.sort_by_key(|stat| {
        let split_partition = stat.partition.split('_').collect::<Vec<&str>>();
        let partition_id = split_partition[1].parse::<u32>().unwrap();
        partition_id
    });

    Ok(StatisticPoints { statistics })
}

// add metadata about new partial table to metadata table
pub async fn add_partition_table_to_metadata_points(
    points_metadata_table: &Table,
    points_metadata_schema: Arc<Schema>,
    columns: Vec<ArrayRef>
) -> Result<(), anyhow::Error> {
    let batches = RecordBatchIterator::new(
        vec![RecordBatch::try_new(points_metadata_schema.clone(), columns)?].into_iter().map(Ok),
        points_metadata_schema.clone()
    );
    let write_params = WriteParams {
        mode: WriteMode::Append,
        ..Default::default()
    };

    points_metadata_table
        .add(batches)
        .mode(AddDataMode::Append)
        .write_options(WriteOptions {
            lance_write_params: Some(write_params),
        })
        .execute().await?;
    points_metadata_table.optimize(OptimizeAction::Compact {
        options: CompactionOptions {
            target_rows_per_fragment: 2 * 1024 * 1024,
            batch_size: Some(5 * 1024 * 1024),
            materialize_deletions_threshold: 0.1,
            ..Default::default()
        },
        remap_options: None,
    }).await?;
    points_metadata_table.optimize(OptimizeAction::Prune {
        older_than: chrono::TimeDelta::try_seconds(1),
        delete_unverified: None,
        error_if_tagged_old_versions: None,
    }).await?;

    Ok(())
}

// update statistics for a specific partial point table
pub async fn update_points_metadata_table(
    points_metadata_table: &Table,
    stats: StatisticsPartitionPoints
) -> Result<(), anyhow::Error> {
    let filter = format!("partition = '{}'", stats.partition);
    let mut updater = points_metadata_table.update().only_if(filter);
    if let Some(id) = stats.id {
        updater = updater.column("id", format!("{}", id));
    }
    if let Some(start) = stats.start {
        updater = updater.column("start", format!("{}", start));
    }
    if let Some(end) = stats.end {
        updater = updater.column("end", format!("{}", end));
    }
    updater.column("count_rows", format!("{}", stats.count_rows)).execute().await?;
    points_metadata_table.optimize(OptimizeAction::Compact {
        options: CompactionOptions {
            target_rows_per_fragment: 2 * 1024 * 1024,
            batch_size: Some(5 * 1024 * 1024),
            materialize_deletions_threshold: 0.1,
            ..Default::default()
        },
        remap_options: None,
    }).await?;
    points_metadata_table.optimize(OptimizeAction::Prune {
        older_than: chrono::TimeDelta::try_seconds(1),
        delete_unverified: None,
        error_if_tagged_old_versions: None,
    }).await?;

    Ok(())
}

// get statistics for a specific partial table
pub async fn get_statistics_by_partition_points_table(
    store_path: &str,
    table_partition_prefix: &str,
    partition_id: u32
) -> Result<StatisticsPartitionPoints, anyhow::Error> {
    let conn = connect(&format!("{store_path}/points")).execute().await?;
    let partition = format!("{}_{}", table_partition_prefix, partition_id);
    let table = conn.open_table(&partition).execute().await?;

    let count_rows = table.count_rows(None).await? as u64;
    let uri = table.dataset_uri();
    let ds = Dataset::open(uri).await?;
    let mut scanner = ds.scan();
    scanner.project(&["timestamp"])?;

    let mut batch_stream = scanner.try_into_stream().await?.map(|b| b.unwrap());
    // MAX = 18446744073709551615
    let mut min_timestamp = std::u64::MAX;
    // MIN = 0
    let mut max_timestamp = std::u64::MIN;
    while let Some(batch) = batch_stream.next().await {
        let array_ts = batch
            .column_by_name("timestamp")
            .ok_or(anyhow::anyhow!("Error get 'timestamp' column arrow array"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow::anyhow!("Error downcast 'timestamp' arrow array to UInt64Array"))?;
        array_ts.iter().for_each(|ts| {
            if let Some(v) = ts {
                if v > max_timestamp {
                    max_timestamp = v;
                }
                if v < min_timestamp {
                    min_timestamp = v;
                }
            }
        });
    }

    Ok(StatisticsPartitionPoints {
        id: Some(partition_id),
        partition,
        start: Some(min_timestamp),
        end: Some(max_timestamp),
        count_rows,
        ..Default::default()
    })
}
