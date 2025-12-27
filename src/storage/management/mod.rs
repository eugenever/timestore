pub mod location;
pub mod measurements_by_stations;
pub mod points;

use std::collections::HashMap;
use std::sync::Arc;

use lancedb::Table;
use nanorand::{ Rng as NanoRNG, WyRand };
use tokio::task::JoinHandle;
use tracing::{ event, Level };

use crate::config::Config;

use super::spawn_compact_table;
use super::models::Measurement;
use super::schema::{
    get_measurements_metadata_schema,
    get_points_metadata_schema,
    get_extended_points_schema,
};

use points::utils::{
    distribute_points_by_partitions,
    filter_measurements_by_standarts,
    save_points_by_partitions,
};
use location::estimate_location;
use measurements_by_stations::{
    MessageMeasurementByStationStorage,
    MessageMetadataMeasurementsByStation,
    init_partition_measurements_by_station_tables,
    measurement_by_station_task,
    save_measurements_by_station_partition_tables,
    update_metadata_measurements_by_stations_task,
    statistics::{
        statistics_partition_measurements_by_station_task,
        MessageStatisticsPartitionMeasurementsByStation,
    },
};
use points::{
    StatisticsPartitionPoints,
    create_partition_points_table,
    get_statistics_by_partition_points_table,
    init_points_table,
    load_points_metadata_table,
    update_points_metadata_table,
};

pub async fn store_management(
    config: &Config,
    rx: flume::Receiver<Vec<Measurement>>,
    metadata_tables: &HashMap<String, Table>,
    store_path: &str
) -> Result<JoinHandle<()>, anyhow::Error> {
    let mut rng = WyRand::new();
    // metadata table of partial point tables
    let points_metadata_table = metadata_tables
        .get("points")
        .ok_or(anyhow::anyhow!("Error get metadata 'points' table"))?;
    // metadata table of partial measurement tables
    let measurements_metadata_table = metadata_tables
        .get("measurements")
        .ok_or(anyhow::anyhow!("Error get metadata 'measurements' table"))?;

    // Prefix for partial tables
    let table_partition_prefix = config.lancedb.table_partition_prefix.clone();
    // point table Schema
    let points_schema = Arc::new(get_extended_points_schema()?);
    // points metadata table Schema
    let points_metadata_schema = Arc::new(get_points_metadata_schema()?);
    // measurements metadata table Schema
    let measurements_metadata_schema = Arc::new(get_measurements_metadata_schema()?);

    // create empty partial point table
    init_points_table(
        store_path,
        &table_partition_prefix,
        points_schema.clone(),
        points_metadata_schema.clone(),
        points_metadata_table
    ).await?;

    let mut statistic_points = load_points_metadata_table(&points_metadata_table).await?;

    let handle = tokio::spawn({
        let max_rows_per_file = config.lancedb.max_rows_per_file;
        let max_rows_per_file_measurement_bs = config.lancedb.max_rows_per_file_measurement_bs;
        let store_path = store_path.to_string();
        let points_metadata_table = points_metadata_table.clone();
        let measurements_metadata_table = measurements_metadata_table.clone();
        let config = config.clone();

        // separate thread pool only for mlat, stack size 8MB
        let pool = threadpool::Builder
            ::new()
            .num_threads(config.mlat.number_threads as usize)
            .thread_stack_size(8_192_000)
            .build();

        let (tx_mss, rx_mss) = flume::unbounded::<MessageMeasurementByStationStorage>();
        let _jh_mt = measurement_by_station_task(store_path.clone(), rx_mss)?;

        let (tx_umss, rx_umss) = flume::unbounded::<MessageMetadataMeasurementsByStation>();
        let _jh_umss = update_metadata_measurements_by_stations_task(rx_umss)?;

        let (tx_spm, rx_spm) =
            flume::unbounded::<MessageStatisticsPartitionMeasurementsByStation>();
        let _jh_spm = statistics_partition_measurements_by_station_task(
            measurements_metadata_table.clone(),
            rx_spm,
            tx_umss.clone()
        )?;

        async move {
            while let Ok(mut measurements) = rx.recv_async().await {
                // TODO: st remove later
                let st = std::time::Instant::now();

                // add a new partial table if the number of rows exceeds the allowed value
                if let Some(current_stat) = statistic_points.statistics.last() {
                    if current_stat.count_rows >= max_rows_per_file {
                        let current_id = current_stat.id.unwrap();
                        let new_partition_id = current_id + 1;

                        // timestamp in seconds
                        let now_ts = chrono::offset::Utc::now().timestamp() as u64;

                        let res = create_partition_points_table(
                            &store_path,
                            &table_partition_prefix,
                            new_partition_id,
                            points_schema.clone(),
                            points_metadata_schema.clone(),
                            &points_metadata_table,
                            now_ts
                        ).await;

                        match res {
                            Err(err) => {
                                event!(Level::ERROR, "Error create partition points table: {err}");
                            }
                            Ok(_table) => {
                                // update the current table, which we close for insertion
                                let stats = StatisticsPartitionPoints {
                                    id: Some(current_id),
                                    partition: format!("{}_{}", table_partition_prefix, current_id),
                                    start: None,
                                    end: Some(now_ts),
                                    count_rows: current_stat.count_rows,
                                    ..Default::default()
                                };

                                if
                                    let Err(err) = update_points_metadata_table(
                                        &points_metadata_table,
                                        stats
                                    ).await
                                {
                                    event!(
                                        Level::ERROR,
                                        "Error update metadata points table: {err}"
                                    );
                                }

                                match load_points_metadata_table(&points_metadata_table).await {
                                    Ok(sp) => {
                                        statistic_points = sp;
                                    }
                                    Err(err) => {
                                        event!(
                                            Level::ERROR,
                                            "Error load points metadata table: {err}"
                                        );
                                    }
                                }
                            }
                        }

                        // spawn the table compression task
                        let dataset = format!("{}/points", store_path);
                        let _jh_ct = spawn_compact_table(
                            &dataset,
                            &table_partition_prefix,
                            current_id
                        );
                    }
                }

                let len_sp = statistic_points.statistics.len();
                measurements.iter_mut().for_each(|m| {
                    // The measurement identifier is a combination of the random ID and timestamp
                    // since we have several distributed storages
                    m.id = Some(rng.generate_range(1_u32..=100_000_000));
                });

                // estimate location
                measurements = estimate_location(&config, &pool, measurements).await;

                // distribute measurements into partitions tables
                let dm = distribute_points_by_partitions(
                    &measurements,
                    statistic_points.clone(),
                    len_sp
                );

                if
                    let Err(err) = save_points_by_partitions(
                        &config,
                        dm,
                        &store_path,
                        points_schema.clone()
                    ).await
                {
                    event!(Level::ERROR, "Error save points by partitions: {err}");
                }

                if let Some(current_stat) = statistic_points.statistics.last() {
                    let current_id = current_stat.id.unwrap();
                    match
                        get_statistics_by_partition_points_table(
                            &store_path,
                            &table_partition_prefix,
                            current_id
                        ).await
                    {
                        Ok(stats) => {
                            // if the table is not empty, we do perform an update
                            if stats.count_rows > 0 {
                                if
                                    let Err(err) = update_points_metadata_table(
                                        &points_metadata_table,
                                        stats.clone()
                                    ).await
                                {
                                    event!(
                                        Level::ERROR,
                                        "Error update points metadata table: {err}"
                                    );
                                }
                                match load_points_metadata_table(&points_metadata_table).await {
                                    Err(err) => {
                                        event!(
                                            Level::ERROR,
                                            "Error load points metadata table: {err}"
                                        );
                                    }
                                    Ok(sp) => {
                                        statistic_points = sp;
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            event!(
                                Level::ERROR,
                                "Error get statistics for points partition id = {current_id}: {err}"
                            );
                        }
                    }
                }

                // ===============================================
                // processing measurements for individual stations
                // ===============================================

                let measurements_by_standarts = filter_measurements_by_standarts(&measurements);

                if
                    let Err(err) = init_partition_measurements_by_station_tables(
                        &measurements_by_standarts,
                        tx_mss.clone(),
                        tx_spm.clone(),
                        &table_partition_prefix,
                        measurements_metadata_schema.clone(),
                        &measurements_metadata_table
                    ).await
                {
                    event!(
                        Level::ERROR,
                        "Error init partition measurements by station tables: {err}"
                    );
                }

                if
                    let Err(err) = save_measurements_by_station_partition_tables(
                        &config,
                        &measurements_by_standarts,
                        &store_path,
                        max_rows_per_file_measurement_bs,
                        &table_partition_prefix,
                        &measurements_metadata_table,
                        measurements_metadata_schema.clone(),
                        tx_umss.clone(),
                        tx_spm.clone()
                    ).await
                {
                    event!(
                        Level::ERROR,
                        "Error save measurements by station partition tables': {err}"
                    );
                }
                // TODO: remove later
                println!("Duration: {:?}", st.elapsed());
            }
        }
    });

    Ok(handle)
}
