use std::env;
use std::fmt::Display;

use arrow_array::{Float64Array, ListArray, StringArray, UInt32Array, UInt64Array};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use lancedb::table::{CompactionOptions, OptimizeAction};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

#[derive(Debug, Default, Clone)]
pub struct StatisticsPartitionPoints {
    pub id: Option<u32>,
    pub partition: String,
    pub start: Option<u64>,
    pub end: Option<u64>,
    pub count_rows: u64,
    pub remote_store: Option<String>,
}

impl Display for StatisticsPartitionPoints {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut start: Option<DateTime<Utc>> = None;
        if let Some(ts) = self.start {
            start = DateTime::from_timestamp(ts as i64, 0);
        }

        let mut end: Option<DateTime<Utc>> = None;
        if let Some(ts) = self.end {
            end = DateTime::from_timestamp(ts as i64, 0);
        }
        write!(
            f,
            "id = {:?}, partition = {}, start = {start:?}, end = {end:?}, count_rows = {}, remote_store = {:?}",
            self.id,
            self.partition,
            self.count_rows,
            self.remote_store
        )
    }
}

// cargo build --package timestore --example store --release
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    read_partition_points().await?;
    read_partition_points_multi().await?;
    read_points_metadata().await?;
    read_measurements_metadata().await?;
    optimize_table("partition_1_compact").await.unwrap();
    Ok(())
}

async fn read_points_metadata() -> Result<(), anyhow::Error> {
    let db = lancedb::connect("./store/metadata").execute().await?;
    let table_name = "points";
    let table = db.open_table(table_name).execute().await?;
    let count_rows = table.count_rows(None).await?;
    let uri = table.dataset_uri();
    let ds = lance::Dataset::open(uri).await?;
    let count_fragments = ds.count_fragments();
    println!("Table '{table_name}': count fragments: {count_fragments}, count rows: {count_rows}");

    let mut scanner = ds.scan();
    scanner.project(&[
        "id",
        "partition",
        "start",
        "end",
        "count_rows",
        "remote_store",
    ])?;

    let mut batch_stream = scanner.try_into_stream().await.unwrap().map(|b| b.unwrap());

    let mut statistics: Vec<StatisticsPartitionPoints> = Vec::with_capacity(count_rows);

    while let Some(batch) = batch_stream.next().await {
        let array_id = batch
            .column_by_name("id")
            .ok_or(anyhow::anyhow!("Error get 'id' arrow array"))?
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'id' arrow array to UInt32Array"
            ))?
            .into_iter()
            .map(|id| id)
            .collect::<Vec<_>>();
        let array_partition = batch
            .column_by_name("partition")
            .ok_or(anyhow::anyhow!("Error get 'partition' arrow array"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'partition' arrow array to StringArray"
            ))?
            .into_iter()
            .map(|p| p.unwrap().to_string())
            .collect::<Vec<_>>();
        let array_start = batch
            .column_by_name("start")
            .ok_or(anyhow::anyhow!("Error get 'start' arrow array"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'start' arrow array to UInt64Array"
            ))?
            .into_iter()
            .map(|s| s)
            .collect::<Vec<_>>();
        let array_end = batch
            .column_by_name("end")
            .ok_or(anyhow::anyhow!("Error get 'end' arrow array"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'end' arrow array to UInt64Array"
            ))?
            .into_iter()
            .map(|e| e)
            .collect::<Vec<_>>();
        let array_cr = batch
            .column_by_name("count_rows")
            .ok_or(anyhow::anyhow!("Error get 'count_rows' arrow array"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'count_rows' arrow array to UInt64Array"
            ))?
            .into_iter()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>();
        let array_rs = batch
            .column_by_name("remote_store")
            .ok_or(anyhow::anyhow!("Error get 'remote_store' arrow array"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'remote_store' arrow array to StringArray"
            ))?
            .into_iter()
            .map(|rs| rs.map(|v| v.to_string()))
            .collect::<Vec<_>>();

        let mut pt: Vec<StatisticsPartitionPoints> = array_id
            .into_iter()
            .zip(array_partition.into_iter())
            .zip(array_start.into_iter())
            .zip(array_end.into_iter())
            .zip(array_cr.into_iter())
            .zip(array_rs.into_iter())
            .map(
                |(((((id, partition), start), end), count_rows), remote_store)| {
                    StatisticsPartitionPoints {
                        id,
                        partition,
                        start,
                        end,
                        count_rows,
                        remote_store,
                    }
                },
            )
            .collect();
        statistics.append(&mut pt);
    }

    println!("Points metadata table:");
    for stat in statistics {
        println!("{stat}");
    }

    Ok(())
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct StatisticsPartitionMeasurementsByStation {
    pub id: Option<u32>,
    pub partition: String,
    pub start: Option<u64>,
    pub end: Option<u64>,
    pub count_rows: u64,
    pub standart: Option<String>,
    pub mcc: Option<u32>,
    pub mnc: Option<u32>,
    pub lac: Option<u32>,
    pub cid: Option<u32>,
}

impl Display for StatisticsPartitionMeasurementsByStation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut start: Option<DateTime<Utc>> = None;
        if let Some(ts) = self.start {
            start = DateTime::from_timestamp(ts as i64, 0);
        }

        let mut end: Option<DateTime<Utc>> = None;
        if let Some(ts) = self.end {
            end = DateTime::from_timestamp(ts as i64, 0);
        }
        write!(
            f,
            "id = {:?}, partition = {}, start = {start:?}, end = {end:?}, count_rows = {}, standart = {:?}, mcc = {:?}, mnc = {:?}, lac = {:?}, cid = {:?}",
            self.id,
            self.partition,
            self.count_rows,
            self.standart,
            self.mcc,
            self.mnc,
            self.lac,
            self.cid
        )
    }
}

async fn read_measurements_metadata() -> Result<(), anyhow::Error> {
    let db = lancedb::connect("./store/metadata").execute().await?;
    let table_name = "measurements";
    let table = db.open_table(table_name).execute().await?;
    let count_rows = table.count_rows(None).await?;
    let uri = table.dataset_uri();
    let ds = lance::Dataset::open(uri).await?;
    let count_fragments = ds.count_fragments();
    println!("Table '{table_name}': count fragments: {count_fragments}, count rows: {count_rows}");

    let mut scanner = ds.scan();
    scanner.project(&[
        "id",
        "partition",
        "start",
        "end",
        "count_rows",
        "standart",
        "mcc",
        "mnc",
        "lac",
        "cid",
    ])?;

    let mut batch_stream = scanner.try_into_stream().await.unwrap().map(|b| b.unwrap());

    let mut statistics: Vec<StatisticsPartitionMeasurementsByStation> =
        Vec::with_capacity(count_rows);

    while let Some(batch) = batch_stream.next().await {
        let array_id = batch
            .column_by_name("id")
            .ok_or(anyhow::anyhow!("Error get 'id' arrow array"))?
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'id' arrow array to UInt32Array"
            ))?
            .into_iter()
            .map(|id| id)
            .collect::<Vec<_>>();
        let array_partition = batch
            .column_by_name("partition")
            .ok_or(anyhow::anyhow!("Error get 'partition' arrow array"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'partition' arrow array to StringArray"
            ))?
            .into_iter()
            .map(|p| p.unwrap().to_string())
            .collect::<Vec<_>>();
        let array_start = batch
            .column_by_name("start")
            .ok_or(anyhow::anyhow!("Error get 'start' arrow array"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'start' arrow array to UInt64Array"
            ))?
            .into_iter()
            .map(|s| s)
            .collect::<Vec<_>>();
        let array_end = batch
            .column_by_name("end")
            .ok_or(anyhow::anyhow!("Error get 'end' arrow array"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'end' arrow array to UInt64Array"
            ))?
            .into_iter()
            .map(|e| e)
            .collect::<Vec<_>>();
        let array_cr = batch
            .column_by_name("count_rows")
            .ok_or(anyhow::anyhow!("Error get 'count_rows' arrow array"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'count_rows' arrow array to UInt64Array"
            ))?
            .into_iter()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>();
        let array_standart = batch
            .column_by_name("standart")
            .ok_or(anyhow::anyhow!("Error get 'standart' arrow array"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'standart' arrow array to StringArray"
            ))?
            .into_iter()
            .map(|rs| rs.map(|v| v.to_string()))
            .collect::<Vec<_>>();
        let array_mcc = batch
            .column_by_name("mcc")
            .ok_or(anyhow::anyhow!("Error get 'mcc' arrow array"))?
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'mcc' arrow array to UInt64Array"
            ))?
            .into_iter()
            .map(|r| r)
            .collect::<Vec<_>>();
        let array_mnc = batch
            .column_by_name("mnc")
            .ok_or(anyhow::anyhow!("Error get 'mnc' arrow array"))?
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'mnc' arrow array to UInt64Array"
            ))?
            .into_iter()
            .map(|r| r)
            .collect::<Vec<_>>();
        let array_lac = batch
            .column_by_name("lac")
            .ok_or(anyhow::anyhow!("Error get 'lac' arrow array"))?
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'lac' arrow array to UInt64Array"
            ))?
            .into_iter()
            .map(|r| r)
            .collect::<Vec<_>>();
        let array_cid = batch
            .column_by_name("cid")
            .ok_or(anyhow::anyhow!("Error get 'cid' arrow array"))?
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'cid' arrow array to UInt64Array"
            ))?
            .into_iter()
            .map(|r| r)
            .collect::<Vec<_>>();

        let mut pt: Vec<StatisticsPartitionMeasurementsByStation> = array_id
            .into_iter()
            .zip(array_partition.into_iter())
            .zip(array_start.into_iter())
            .zip(array_end.into_iter())
            .zip(array_cr.into_iter())
            .zip(array_standart.into_iter())
            .zip(array_mcc.into_iter())
            .zip(array_mnc.into_iter())
            .zip(array_lac.into_iter())
            .zip(array_cid.into_iter())
            .map(
                |(
                    (
                        (
                            (
                                (((((id, partition), start), end), count_rows), standart),
                                mcc,
                            ),
                            mnc,
                        ),
                        lac,
                    ),
                    cid,
                )| {
                    StatisticsPartitionMeasurementsByStation {
                        id,
                        partition,
                        start,
                        end,
                        count_rows,
                        standart,
                        mcc,
                        mnc,
                        lac,
                        cid,
                    }
                },
            )
            .collect();
        statistics.append(&mut pt);
    }

    println!("Measurements metadata table:");
    for stat in statistics {
        println!("{stat}");
    }

    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
struct MeasurementFromDB {
    id: Option<u32>,
    unit_id: Option<String>,
    timestamp: u64,
    longitude: f64,
    latitude: f64,
    x: Option<f64>,
    y: Option<f64>,
    height_agl: Option<f64>,
    height_asl: Option<f64>,
    base_stations: Option<Vec<Option<String>>>,
}

async fn read_partition_points_multi() -> Result<(), anyhow::Error> {
    let mut set = JoinSet::new();
    for _ in 0..100 {
        set.spawn(read_partition_points());
    }

    while let Some(res) = set.join_next().await {
        let out = res?;
        if let Err(err) = out {
            println!("Error read part multi: {:?}", err);
        }
    }
    Ok(())
}

async fn read_partition_points() -> Result<(), anyhow::Error> {
    let args: Vec<String> = env::args().collect();
    let min_ts: String;
    let max_ts: String;
    let partition: String;
    if args.len() == 4 {
        min_ts = args[1].clone();
        max_ts = args[2].clone();
        partition = args[3].clone();
    } else {
        min_ts = "1743171889".to_string();
        max_ts = "1743173889".to_string();
        partition = "partition_1".to_string();
    }

    let st = std::time::Instant::now();
    let db = lancedb::connect("./store/points").execute().await?;
    let table = db.open_table(partition.clone()).execute().await?;
    let count_rows = table.count_rows(None).await?;
    let uri = table.dataset_uri();
    let ds = lance::Dataset::open(uri).await?;
    let count_fragments = ds.count_fragments();
    println!(
        "Table '{}': count fragments: {count_fragments}, count rows: {count_rows}",
        partition.as_str()
    );

    let mut scanner = ds.scan();
    scanner
        .project(&[
            "id",
            "unit_id",
            "timestamp",
            "longitude",
            "latitude",
            "x",
            "y",
            "height_agl",
            "height_asl",
            "base_stations",
        ])?
        .filter(&format!(
            "timestamp >= {} AND timestamp <= {}",
            min_ts, max_ts
        ))?;
    // .limit(Some(100), None)?;

    let mut batch_stream = scanner.try_into_stream().await?.map(|b| b.unwrap());

    let mut ms: Vec<MeasurementFromDB> = Vec::with_capacity(count_rows);

    while let Some(batch) = batch_stream.next().await {
        let array_id = batch
            .column_by_name("id")
            .ok_or(anyhow::anyhow!("Error get 'id' arrow array"))?
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'id' arrow array to UInt32Array"
            ))?
            .into_iter()
            .map(|id| id)
            .collect::<Vec<_>>();
        let array_unit_id = batch
            .column_by_name("unit_id")
            .ok_or(anyhow::anyhow!("Error get 'unit_id' arrow array"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'unit_id' arrow array to StringArray"
            ))?
            .into_iter()
            .map(|p| Some(p.unwrap().to_string()))
            .collect::<Vec<_>>();
        let array_timestamp = batch
            .column_by_name("timestamp")
            .ok_or(anyhow::anyhow!("Error get 'timestamp' arrow array"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'timestamp' arrow array to UInt64Array"
            ))?
            .into_iter()
            .map(|s| s.unwrap())
            .collect::<Vec<_>>();
        let array_longitude = batch
            .column_by_name("longitude")
            .ok_or(anyhow::anyhow!("Error get 'longitude' arrow array"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'longitude' arrow array to Float64Array"
            ))?
            .into_iter()
            .map(|e| e.unwrap())
            .collect::<Vec<_>>();
        let array_latitude = batch
            .column_by_name("latitude")
            .ok_or(anyhow::anyhow!("Error get 'latitude' arrow array"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'latitude' arrow array to Float64Array"
            ))?
            .into_iter()
            .map(|e| e.unwrap())
            .collect::<Vec<_>>();
        let array_x = batch
            .column_by_name("x")
            .ok_or(anyhow::anyhow!("Error get 'x' arrow array"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'x' arrow array to Float64Array"
            ))?
            .into_iter()
            .map(|e| e)
            .collect::<Vec<_>>();
        let array_y = batch
            .column_by_name("y")
            .ok_or(anyhow::anyhow!("Error get 'y' arrow array"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'y' arrow array to Float64Array"
            ))?
            .into_iter()
            .map(|e| e)
            .collect::<Vec<_>>();
        let array_hagl = batch
            .column_by_name("height_agl")
            .ok_or(anyhow::anyhow!("Error get 'height_agl' arrow array"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'height_agl' arrow array to Float64Array"
            ))?
            .into_iter()
            .map(|e| e)
            .collect::<Vec<_>>();
        let array_hasl = batch
            .column_by_name("height_asl")
            .ok_or(anyhow::anyhow!("Error get 'height_asl' arrow array"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or(anyhow::anyhow!(
                "Error downcast 'height_asl' arrow array to Float64Array"
            ))?
            .into_iter()
            .map(|e| e)
            .collect::<Vec<_>>();
        let array_bs = batch
            .column_by_name("base_stations")
            .ok_or(anyhow::anyhow!("Error get 'base_stations' arrow array"))?
            .as_any()
            .downcast_ref::<ListArray>()
            .map(|data| {
                data.iter()
                    .map(|data| {
                        data.map(|data| {
                            data.as_any()
                                .downcast_ref::<StringArray>()
                                .and_then(|data| {
                                    data.iter()
                                        .map(|e| e.map(|v| Some(v.to_string())))
                                        .collect::<Option<Vec<Option<String>>>>()
                                })
                        })
                    })
                    .collect::<Option<Vec<Option<Vec<Option<String>>>>>>()
            })
            .flatten()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Error downcast 'base_station' arrow array to Float64Array"
                )
            })?;

        let mut m: Vec<MeasurementFromDB> = array_id
            .into_iter()
            .zip(array_unit_id.into_iter())
            .zip(array_timestamp.into_iter())
            .zip(array_longitude.into_iter())
            .zip(array_latitude.into_iter())
            .zip(array_x.into_iter())
            .zip(array_y.into_iter())
            .zip(array_hagl.into_iter())
            .zip(array_hasl.into_iter())
            .zip(array_bs.into_iter())
            .map(
                |(((((((((id, uid), ts), lon), lat), x), y), hagl), hasl), bs)| {
                    MeasurementFromDB {
                        id,
                        unit_id: uid,
                        timestamp: ts,
                        longitude: lon,
                        latitude: lat,
                        x,
                        y,
                        height_agl: hagl,
                        height_asl: hasl,
                        base_stations: bs,
                    }
                },
            )
            .collect();
        ms.append(&mut m);
    }

    // println!("{:?}", ms);
    println!("Duration read {} points: {:?}", ms.len(), st.elapsed());

    Ok(())
}

async fn optimize_table(partition: &str) -> Result<(), ()> {
    let db = lancedb::connect("./store/points").execute().await.unwrap();
    let table = db.open_table(partition).execute().await.unwrap();
    table
        .optimize(OptimizeAction::Compact {
            options: CompactionOptions {
                target_rows_per_fragment: 2 * 1024 * 1024,
                batch_size: Some(2 * 1024 * 1024),
                materialize_deletions_threshold: 0.1,
                ..Default::default()
            },
            remap_options: None,
        })
        .await
        .unwrap();
    table
        .optimize(OptimizeAction::Prune {
            older_than: chrono::TimeDelta::try_seconds(1),
            delete_unverified: None,
            error_if_tagged_old_versions: None,
        })
        .await
        .unwrap();
    Ok(())
}
