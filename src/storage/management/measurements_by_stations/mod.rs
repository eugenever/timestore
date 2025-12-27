pub mod save_measurements;
pub mod statistics;

use std::collections::{ HashMap, HashSet };
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::{ ArrayRef, StringArray, UInt32Array, UInt64Array };
use arrow_schema::Schema;
use futures::StreamExt;
use lance::Dataset;
use lancedb::table::{ CompactionOptions, OptimizeAction };
use lancedb::{ connect, Table };

use polars::{ frame::DataFrame, series::Series };
/*
    use polars::{ frame::DataFrame, lazy::{ dsl::{ col, lit }, prelude::* }, series::Series };
*/

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{ event, Level };

use crate::config::Config;
use crate::storage::{
    spawn_compact_table,
    conversion::arrow_array_to_polars_series,
    models::{ BSCodes, Measurement, MeasurementsByStandarts },
    schema::{ get_schema_by_standart, Standart },
};

use save_measurements::{
    save_five_gnr_measurements_by_partitions,
    save_gsm_measurements_by_partitions,
    save_lte_measurements_by_partitions,
    save_wcdma_measurements_by_partitions,
};
use statistics::{
    StatisticsPartitionMeasurementsByStation,
    MessageStatisticsPartitionMeasurementsByStation,
    send_update_message,
    send_get_bs_stats_message,
};
use crate::storage::save_batches;

#[derive(Debug)]
pub enum MessageMeasurementByStationStorage {
    GetMeasurementDirectory {
        standart: String,
        bs_codes: BSCodes,
        tx: oneshot::Sender<Option<String>>,
    },
}

// update statistics for a specific partial measurement by station table
pub async fn update_metadata_measurements_by_stations(
    measurements_metadata_table: &Table,
    stats: StatisticsPartitionMeasurementsByStation
) -> Result<(), anyhow::Error> {
    measurements_metadata_table.checkout_latest().await?;
    if
        stats.standart.is_none() ||
        stats.mcc.is_none() ||
        stats.mnc.is_none() ||
        stats.lac.is_none() ||
        stats.cid.is_none()
    {
        return Err(
            anyhow::anyhow!(
                "Error 'update_metadata_measurements_by_stations': statistics contains None values: {stats:?}"
            )
        );
    }
    let filter = format!(
        "partition = '{}' AND standart = '{}' AND mcc = {} AND mnc = {} AND lac = {} AND cid = {}",
        stats.partition,
        stats.standart.unwrap(),
        stats.mcc.unwrap(),
        stats.mnc.unwrap(),
        stats.lac.unwrap(),
        stats.cid.unwrap()
    );
    let mut updater = measurements_metadata_table.update().only_if(filter);
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

    measurements_metadata_table.optimize(OptimizeAction::Compact {
        options: CompactionOptions {
            target_rows_per_fragment: 2 * 1024 * 1024,
            batch_size: Some(5 * 1024 * 1024),
            materialize_deletions_threshold: 0.1,
            ..Default::default()
        },
        remap_options: None,
    }).await?;
    measurements_metadata_table.optimize(OptimizeAction::Prune {
        older_than: chrono::TimeDelta::try_seconds(1),
        delete_unverified: None,
        error_if_tagged_old_versions: None,
    }).await?;

    Ok(())
}

// load all partial measurements by stations tables into memory
pub async fn load_measurements_by_station_metadata_table(
    measurements_metadata_table: &Table
) -> Result<DataFrame, anyhow::Error> {
    measurements_metadata_table.checkout_latest().await?;
    let uri = measurements_metadata_table.dataset_uri();
    let ds = Dataset::open(uri).await?;
    let mut scanner = ds.scan();
    scanner.project(
        &["id", "partition", "start", "end", "count_rows", "standart", "mcc", "mnc", "lac", "cid"]
    )?;

    let mut id_series = Series::new_empty("id", &polars::datatypes::DataType::UInt32);
    let mut partition_series = Series::new_empty("partition", &polars::datatypes::DataType::String);
    let mut start_series = Series::new_empty("start", &polars::datatypes::DataType::UInt64);
    let mut end_series = Series::new_empty("end", &polars::datatypes::DataType::UInt64);
    let mut cr_series = Series::new_empty("count_rows", &polars::datatypes::DataType::UInt64);
    let mut standart_series = Series::new_empty("standart", &polars::datatypes::DataType::String);
    let mut mcc_series = Series::new_empty("mcc", &polars::datatypes::DataType::UInt32);
    let mut mnc_series = Series::new_empty("mnc", &polars::datatypes::DataType::UInt32);
    let mut lac_series = Series::new_empty("lac", &polars::datatypes::DataType::UInt32);
    let mut cid_series = Series::new_empty("cid", &polars::datatypes::DataType::UInt32);

    let mut batch_stream = scanner.try_into_stream().await?.map(|b| b.unwrap());
    while let Some(batch) = batch_stream.next().await {
        arrow_array_to_polars_series(&batch, "id", &mut id_series)?;
        arrow_array_to_polars_series(&batch, "partition", &mut partition_series)?;
        arrow_array_to_polars_series(&batch, "start", &mut start_series)?;
        arrow_array_to_polars_series(&batch, "end", &mut end_series)?;
        arrow_array_to_polars_series(&batch, "count_rows", &mut cr_series)?;
        arrow_array_to_polars_series(&batch, "standart", &mut standart_series)?;
        arrow_array_to_polars_series(&batch, "mcc", &mut mcc_series)?;
        arrow_array_to_polars_series(&batch, "mnc", &mut mnc_series)?;
        arrow_array_to_polars_series(&batch, "lac", &mut lac_series)?;
        arrow_array_to_polars_series(&batch, "cid", &mut cid_series)?;
    }

    let dataframe = DataFrame::new(
        vec![
            id_series,
            partition_series,
            start_series,
            end_series,
            cr_series,
            standart_series,
            mcc_series,
            mnc_series,
            lac_series,
            cid_series
        ]
    )?;

    Ok(dataframe)
}

pub async fn init_partition_measurements_by_station_tables(
    measurements_by_standarts: &MeasurementsByStandarts<'_>,
    tx_mss: flume::Sender<MessageMeasurementByStationStorage>,
    tx_spm: flume::Sender<MessageStatisticsPartitionMeasurementsByStation>,
    table_partition_prefix: &str,
    measurements_metadata_schema: Arc<Schema>,
    measurements_metadata_table: &Table
) -> Result<(), anyhow::Error> {
    for (standart_str, bs_codes) in &measurements_by_standarts.bs_codes_by_standarts {
        let standart = Standart::from_str(standart_str)?;
        let ms = get_schema_by_standart(&standart)?;

        for codes in bs_codes {
            let (tx, rx) = oneshot::channel();
            if
                let Err(err) = tx_mss.send_async(
                    MessageMeasurementByStationStorage::GetMeasurementDirectory {
                        standart: standart_str.clone(),
                        bs_codes: codes.clone(),
                        tx,
                    }
                ).await
            {
                event!(Level::ERROR, "Error send get measurement directory message: {err}");
            }

            match rx.await {
                Err(err) => {
                    event!(Level::ERROR, "Error receive get measurement directory message: {err}");
                }
                Ok(db_bs) => {
                    // db = CWD/store/measurements/GSM/250/99/2564/15243
                    //                       /STANDART/MCC/MNC/LAC/CID
                    if let Some(db) = db_bs {
                        let conn = connect(&db).execute().await?;
                        let tables = conn.table_names().execute().await?;
                        // if the storage does not have tables, create the first partial table
                        if tables.len() == 0 {
                            // current timestamp minus 1 year (seconds)
                            let now_ts =
                                (chrono::offset::Utc::now().timestamp() as u64) - 31_536_000;
                            // Add meta info
                            let id = 1;
                            let count_rows = 0;
                            let partition = format!("{}_{}", table_partition_prefix, id);

                            conn.create_empty_table(&partition, ms.clone()).execute().await?;

                            let remote_store: Vec<Option<String>> = vec![None];
                            let columns: Vec<ArrayRef> = vec![
                                // id = 1
                                Arc::new(UInt32Array::from(vec![id])),
                                // partition = partition_1
                                Arc::new(StringArray::from(vec![partition])),
                                // start = now timestamp in seconds
                                Arc::new(UInt64Array::from(vec![Some(now_ts)])),
                                // end = Null
                                Arc::new(UInt64Array::from(vec![None])),
                                // count_rows = 0
                                Arc::new(UInt64Array::from(vec![count_rows])),
                                // remote_store
                                Arc::new(StringArray::from(remote_store)),
                                // standart = standart
                                Arc::new(StringArray::from(vec![standart_str.clone()])),
                                // MCC
                                Arc::new(UInt32Array::from(vec![codes.mcc])),
                                // MNC
                                Arc::new(UInt32Array::from(vec![codes.mnc])),
                                // LAC
                                Arc::new(UInt32Array::from(vec![codes.lac])),
                                // CID
                                Arc::new(UInt32Array::from(vec![codes.cid]))
                            ];
                            let stat = StatisticsPartitionMeasurementsByStation {
                                id: Some(id),
                                partition: format!("{}_{}", table_partition_prefix, id),
                                start: Some(now_ts),
                                end: None,
                                count_rows,
                                standart: Some(standart_str.clone()),
                                mcc: Some(codes.mcc),
                                mnc: Some(codes.mnc),
                                lac: Some(codes.lac),
                                cid: Some(codes.cid),
                            };
                            add_partition_table_to_measurements_metadata(
                                measurements_metadata_table,
                                measurements_metadata_schema.clone(),
                                columns,
                                stat,
                                None,
                                tx_spm.clone()
                            ).await?;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

// add metadata about new partial table of base station measurements to metadata table
pub async fn add_partition_table_to_measurements_metadata(
    measurements_metadata_table: &Table,
    measurements_metadata_schema: Arc<Schema>,
    columns: Vec<ArrayRef>,
    stat: StatisticsPartitionMeasurementsByStation,
    tx_umss: Option<flume::Sender<MessageMetadataMeasurementsByStation>>,
    tx_spm: flume::Sender<MessageStatisticsPartitionMeasurementsByStation>
) -> Result<(), anyhow::Error> {
    if let Some(tx) = tx_umss {
        let (tx_submit, rx_submit) = oneshot::channel::<()>();
        match
            tx.send_async(MessageMetadataMeasurementsByStation::Create {
                measurements_metadata_table: measurements_metadata_table.clone(),
                measurements_metadata_schema,
                columns,
                tx_submit,
            }).await
        {
            Err(err) => {
                event!(Level::ERROR, "Error send create metadata measurements message: {err}");
            }
            Ok(_) => {
                if let Err(err) = rx_submit.await {
                    event!(
                        Level::ERROR,
                        "Error receive submit on create metadata measurements: {err}"
                    );
                }
            }
        }
    } else {
        save_batches(measurements_metadata_table, measurements_metadata_schema, columns).await?;
    }
    send_update_message(tx_spm, stat).await?;

    Ok(())
}

pub async fn save_measurements_by_station_partition_tables(
    config: &Config,
    measurements_by_standarts: &MeasurementsByStandarts<'_>,
    store_path: &str,
    max_rows_per_file_measurement_bs: u64,
    table_partition_prefix: &str,
    measurements_metadata_table: &Table,
    measurements_metadata_schema: Arc<Schema>,
    tx_umss: flume::Sender<MessageMetadataMeasurementsByStation>,
    tx_spm: flume::Sender<MessageStatisticsPartitionMeasurementsByStation>
) -> Result<(), anyhow::Error> {
    let mut handles = Vec::new();
    for (
        standart_str,
        bs_codes_measurements,
    ) in &measurements_by_standarts.measurements_by_standarts_and_bs_codes {
        for (bs_codes, measuremenets) in bs_codes_measurements {
            let mut stats = send_get_bs_stats_message(
                tx_spm.clone(),
                standart_str,
                bs_codes
            ).await?;

            let standart = Standart::from_str(standart_str)?;
            let measurements_schema = get_schema_by_standart(&standart)?;

            // rarely called functionality
            if let Some(current_stat) = stats.last() {
                if current_stat.count_rows >= max_rows_per_file_measurement_bs {
                    let current_id = current_stat.id.unwrap();
                    let new_partition_id = current_id + 1;
                    // timestamp in seconds
                    let now_ts = chrono::offset::Utc::now().timestamp() as u64;

                    let res = create_partition_measurements_by_stations_table(
                        &store_path,
                        &bs_codes,
                        standart_str,
                        table_partition_prefix,
                        new_partition_id,
                        measurements_schema.clone(),
                        measurements_metadata_schema.clone(),
                        measurements_metadata_table,
                        now_ts,
                        tx_umss.clone(),
                        tx_spm.clone()
                    ).await;

                    match res {
                        Err(err) => {
                            event!(
                                Level::ERROR,
                                "Error create partition table for measurements by stations: {err}"
                            );
                        }
                        Ok(result) => {
                            if let Some(_table) = result {
                                // update the current table, which we close for insertion
                                let stat = StatisticsPartitionMeasurementsByStation {
                                    id: Some(current_id),
                                    partition: format!("{}_{}", table_partition_prefix, current_id),
                                    end: Some(now_ts),
                                    count_rows: current_stat.count_rows,
                                    standart: Some(standart_str.clone()),
                                    mcc: Some(bs_codes.mcc),
                                    mnc: Some(bs_codes.mnc),
                                    lac: Some(bs_codes.lac),
                                    cid: Some(bs_codes.cid),
                                    ..Default::default()
                                };
                                send_update_message(tx_spm.clone(), stat).await?;

                                // statistics for new partial table
                                let stat_new_partial_table =
                                    StatisticsPartitionMeasurementsByStation {
                                        id: Some(new_partition_id),
                                        partition: format!(
                                            "{}_{}",
                                            table_partition_prefix,
                                            new_partition_id
                                        ),
                                        start: Some(now_ts),
                                        end: None,
                                        count_rows: 0,
                                        standart: Some(standart_str.clone()),
                                        mcc: Some(bs_codes.mcc),
                                        mnc: Some(bs_codes.mnc),
                                        lac: Some(bs_codes.lac),
                                        cid: Some(bs_codes.cid),
                                    };
                                send_update_message(tx_spm.clone(), stat_new_partial_table).await?;

                                // request updated statistics
                                stats = send_get_bs_stats_message(
                                    tx_spm.clone(),
                                    standart_str,
                                    bs_codes
                                ).await?;
                            }
                        }
                    }

                    // spawn the table compression task
                    let dataset = format!(
                        "{}/measurements/{}/{}/{}/{}/{}",
                        store_path,
                        standart_str,
                        bs_codes.mcc,
                        bs_codes.mnc,
                        bs_codes.lac,
                        bs_codes.cid
                    );
                    let _jh_ct = spawn_compact_table(&dataset, &table_partition_prefix, current_id);
                }
            }

            let len_stats = stats.len();
            // we distribute measurements by stations according to partial tables of the specified stations
            // high IO load
            let dm = distribute_measurements_by_stations_by_partitions(
                &measuremenets,
                &stats,
                len_stats
            );

            match standart {
                Standart::GSM => {
                    let gsm_handles = save_gsm_measurements_by_partitions(
                        config,
                        dm,
                        store_path,
                        bs_codes,
                        measurements_schema,
                        tx_spm.clone()
                    ).await?;
                    handles.extend(gsm_handles);
                }
                Standart::WCDMA => {
                    let wcdma_handles = save_wcdma_measurements_by_partitions(
                        config,
                        dm,
                        store_path,
                        bs_codes,
                        measurements_schema,
                        tx_spm.clone()
                    ).await?;
                    handles.extend(wcdma_handles);
                }
                Standart::LTE => {
                    let lte_handles = save_lte_measurements_by_partitions(
                        config,
                        dm,
                        store_path,
                        bs_codes,
                        measurements_schema,
                        tx_spm.clone()
                    ).await?;
                    handles.extend(lte_handles);
                }
                Standart::FiveGNR => {
                    let five_gnr_handles = save_five_gnr_measurements_by_partitions(
                        config,
                        dm,
                        store_path,
                        bs_codes,
                        measurements_schema,
                        tx_spm.clone()
                    ).await?;
                    handles.extend(five_gnr_handles);
                }
            }
        }
    }

    for handle in handles {
        if let Err(err) = handle.await {
            event!(Level::ERROR, "Error save measurements by stations from task: {err}");
        }
    }

    Ok(())
}

fn search_target_measurements_by_station_partition(
    ts: u64,
    stats: &[StatisticsPartitionMeasurementsByStation],
    len_stats: usize
) -> Result<&String, anyhow::Error> {
    for (i, stat) in stats.iter().enumerate() {
        let partition = &stat.partition;
        if let Some(start) = stat.start {
            if let Some(end) = stat.end {
                // write in a closed partition table as needed
                if ts >= start && ts <= end {
                    return Ok(partition);
                }
            } else if ts >= start && i == len_stats - 1 {
                // write to the currently open table
                return Ok(partition);
            }
        }
    }

    Err(anyhow::anyhow!("Error search measurements by station partition by timestamp {ts}"))
}

// creating new partial point table and adding it to the metadata
pub async fn create_partition_measurements_by_stations_table(
    store_path: &str,
    bs_codes: &BSCodes,
    standart: &str,
    table_partition_prefix: &str,
    partition_id: u32,
    measurements_schema: Arc<Schema>,
    measurements_metadata_schema: Arc<Schema>,
    measurements_metadata_table: &Table,
    start: u64,
    tx_umss: flume::Sender<MessageMetadataMeasurementsByStation>,
    tx_spm: flume::Sender<MessageStatisticsPartitionMeasurementsByStation>
) -> Result<Option<Table>, anyhow::Error> {
    let (_, db_directory) = database_station_directory_from_codes(store_path, standart, bs_codes);
    let conn = connect(&db_directory).execute().await?;
    let partition = format!("{}_{}", table_partition_prefix, partition_id);
    let table = conn.create_empty_table(&partition, measurements_schema).execute().await?;
    let count_rows = 0;
    let remote_store: Vec<Option<String>> = vec![None];
    let columns: Vec<ArrayRef> = vec![
        // id = partition_id
        Arc::new(UInt32Array::from(vec![partition_id])),
        // partition = partition
        Arc::new(StringArray::from(vec![partition.clone()])),
        // start = now timestamp in seconds
        Arc::new(UInt64Array::from(vec![Some(start)])),
        // end = Null
        Arc::new(UInt64Array::from(vec![None])),
        // count_rows = 0
        Arc::new(UInt64Array::from(vec![count_rows])),
        // remote_store = Null
        Arc::new(StringArray::from(remote_store)),
        // standart = standart
        Arc::new(StringArray::from(vec![standart])),
        // MCC
        Arc::new(UInt32Array::from(vec![bs_codes.mcc])),
        // MNC
        Arc::new(UInt32Array::from(vec![bs_codes.mnc])),
        // LAC
        Arc::new(UInt32Array::from(vec![bs_codes.lac])),
        // CID
        Arc::new(UInt32Array::from(vec![bs_codes.cid]))
    ];
    let stat = StatisticsPartitionMeasurementsByStation {
        id: Some(partition_id),
        partition,
        start: Some(start),
        end: None,
        count_rows,
        standart: Some(standart.to_string()),
        mcc: Some(bs_codes.mcc),
        mnc: Some(bs_codes.mnc),
        lac: Some(bs_codes.lac),
        cid: Some(bs_codes.cid),
    };
    add_partition_table_to_measurements_metadata(
        measurements_metadata_table,
        measurements_metadata_schema.clone(),
        columns,
        stat,
        Some(tx_umss.clone()),
        tx_spm
    ).await?;

    Ok(Some(table))
}

// distribute measurements by stations into partitions tables
pub fn distribute_measurements_by_stations_by_partitions<'a>(
    measurements: &'a [&'a Measurement],
    stats: &[StatisticsPartitionMeasurementsByStation],
    len_stats: usize
) -> HashMap<String, Vec<&'a Measurement>> {
    let mut dm: HashMap<String, Vec<&Measurement>> = HashMap::new();
    let default_partition = stats
        .last()
        .map(|stat| &stat.partition)
        .unwrap();
    for m in measurements {
        let timestamp_m = m.timestamp.timestamp() as u64;
        let target_partition =
            // TODO: work out the default partition
            search_target_measurements_by_station_partition(
                timestamp_m,
                stats,
                len_stats
            ).unwrap_or(default_partition);
        if let Some(ms) = dm.get_mut(target_partition) {
            ms.push(m);
        } else {
            let mut ms = Vec::with_capacity(measurements.len());
            ms.push(*m);
            dm.insert(target_partition.clone(), ms);
        }
    }

    dm
}

pub async fn create_measurements_by_station_directory(bs_dir: &str) -> Result<(), anyhow::Error> {
    match tokio::fs::try_exists(bs_dir).await {
        Err(err) => {
            return Err(
                anyhow::anyhow!(
                    "Error check measurements by station directory '{bs_dir}' exists: {}",
                    err
                )
            );
        }
        Ok(exist) => {
            if !exist {
                if let Err(err) = tokio::fs::create_dir_all(bs_dir).await {
                    return Err(
                        anyhow::anyhow!(
                            "Error init measurements by station directory '{bs_dir}': {}",
                            err
                        )
                    );
                }
            }
        }
    }

    Ok(())
}

pub fn measurement_by_station_task(
    store_path: String,
    rx_mss: flume::Receiver<MessageMeasurementByStationStorage>
) -> Result<JoinHandle<Result<(), anyhow::Error>>, anyhow::Error> {
    let jh = tokio::spawn(async move {
        let mut standart_codes_dirs: HashMap<String, HashSet<String>> = HashMap::new();
        while let Ok(message) = rx_mss.recv_async().await {
            match message {
                // reduces the number of calls to the file system when checking directories
                MessageMeasurementByStationStorage::GetMeasurementDirectory {
                    standart,
                    bs_codes,
                    tx,
                } => {
                    let (codes_dir, bs_dir) = database_station_directory_from_codes(
                        &store_path,
                        &standart,
                        &bs_codes
                    );

                    if let Some(codes_dirs) = standart_codes_dirs.get_mut(&standart) {
                        if codes_dirs.contains(&codes_dir) {
                            if let Err(err) = tx.send(Some(bs_dir)) {
                                event!(
                                    Level::ERROR,
                                    "Error send station directory {}",
                                    err.unwrap()
                                );
                            }
                            continue;
                        }
                    } else {
                        let codes_dirs = HashSet::new();
                        standart_codes_dirs.insert(standart.clone(), codes_dirs);
                    }

                    let mut db_bs: Option<String> = None;
                    if let Err(err) = create_measurements_by_station_directory(&bs_dir).await {
                        event!(Level::ERROR, "Error create station directory '{bs_dir}': {}", err);
                    } else {
                        db_bs = Some(bs_dir);
                        if let Some(codes_dirs) = standart_codes_dirs.get_mut(&standart) {
                            codes_dirs.insert(codes_dir);
                        }
                    }
                    if let Err(err) = tx.send(db_bs) {
                        event!(Level::ERROR, "Error send station directory {}", err.unwrap());
                    }
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    Ok(jh)
}

pub fn database_station_directory_from_codes(
    store_path: &str,
    standart: &str,
    bs_codes: &BSCodes
) -> (String, String) {
    let codes_dir = format!(
        "{standart}/{}/{}/{}/{}",
        bs_codes.mcc,
        bs_codes.mnc,
        bs_codes.lac,
        bs_codes.cid
    );
    let database_bs_dir = format!("{store_path}/measurements/{}", codes_dir);

    (codes_dir, database_bs_dir)
}

pub enum MessageMetadataMeasurementsByStation {
    Update {
        measurements_metadata_table: Table,
        stat: StatisticsPartitionMeasurementsByStation,
        tx_submit: Option<oneshot::Sender<()>>,
    },
    Create {
        measurements_metadata_table: Table,
        measurements_metadata_schema: Arc<Schema>,
        columns: Vec<ArrayRef>,
        tx_submit: oneshot::Sender<()>,
    },
}

pub fn update_metadata_measurements_by_stations_task(
    rx: flume::Receiver<MessageMetadataMeasurementsByStation>
) -> Result<JoinHandle<Result<(), anyhow::Error>>, anyhow::Error> {
    let jh = tokio::spawn(async move {
        while let Ok(message) = rx.recv_async().await {
            match message {
                MessageMetadataMeasurementsByStation::Update {
                    measurements_metadata_table,
                    stat,
                    tx_submit,
                } => {
                    if
                        let Err(err) = update_metadata_measurements_by_stations(
                            &measurements_metadata_table,
                            stat
                        ).await
                    {
                        event!(
                            Level::ERROR,
                            "Error update metadata measurements by stations table in task: {err}"
                        );
                    }
                    if let Some(tx) = tx_submit {
                        if let Err(_err) = tx.send(()) {
                            event!(Level::ERROR, "Error send submit update message");
                        }
                    }
                }
                MessageMetadataMeasurementsByStation::Create {
                    measurements_metadata_table,
                    measurements_metadata_schema,
                    columns,
                    tx_submit,
                } => {
                    save_batches(
                        &measurements_metadata_table,
                        measurements_metadata_schema,
                        columns
                    ).await?;
                    if let Err(_err) = tx_submit.send(()) {
                        event!(Level::ERROR, "Error send submit create message");
                    }
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    });

    Ok(jh)
}
