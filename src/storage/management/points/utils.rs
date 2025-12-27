#![allow(unused)]
use std::collections::{ HashMap, HashSet };
use std::sync::Arc;

use arrow::array::{
    Float64Array,
    StringArray,
    UInt32Array,
    UInt64Array,
    GenericListArray,
    ListBuilder,
    StringBuilder,
};
use arrow::record_batch::{ RecordBatch, RecordBatchIterator };
use arrow_schema::Schema;

use lance::dataset::{ WriteMode, WriteParams };
use lancedb::connect;
use lancedb::table::{ AddDataMode, CompactionOptions, OptimizeAction, WriteOptions };

use chrono::Utc;
use serde::{ Deserialize, Serialize };
use strum::IntoEnumIterator;
use tracing::{ event, Level };

use crate::config::Config;

use super::StatisticPoints;
use crate::storage::models::{ BSCodes, Measurement, MeasurementsByStandarts };
use crate::storage::schema::Standart;

// 1. filter measurements by standards
// 2. group base stations by MCC, MNC, LAC, CID
pub fn filter_measurements_by_standarts(measurements: &[Measurement]) -> MeasurementsByStandarts {
    let capacity = Standart::iter().count();
    let mut measurements_by_standarts: HashMap<String, Vec<&Measurement>> = HashMap::with_capacity(
        capacity
    );
    let mut bs_codes_by_standarts: HashMap<String, HashSet<BSCodes>> = HashMap::with_capacity(
        capacity
    );
    let mut measurements_by_standarts_and_bs_codes: HashMap<
        String,
        HashMap<BSCodes, Vec<&Measurement>>
    > = HashMap::with_capacity(capacity);

    for standart in Standart::iter() {
        let standart_str = standart.to_string();
        let filtered_measurement: Vec<&Measurement>;
        let mut set_codes = HashSet::new();
        match standart {
            Standart::GSM => {
                filtered_measurement = measurements
                    .iter()
                    .filter(|m| m.gsm.is_some())
                    .collect::<Vec<_>>();
                if filtered_measurement.len() > 0 {
                    let bs_codes_measurements = HashMap::new();
                    measurements_by_standarts_and_bs_codes.insert(
                        standart_str.clone(),
                        bs_codes_measurements
                    );
                }
                filtered_measurement.iter().for_each(|m| {
                    if let Some(gsm) = m.gsm.as_ref() {
                        gsm.iter().for_each(|gsm_m| {
                            let bs_codes = BSCodes {
                                mcc: gsm_m.mcc,
                                mnc: gsm_m.mnc,
                                lac: gsm_m.lac,
                                cid: gsm_m.cid,
                            };
                            if
                                let Some(bs_codes_measurements) =
                                    measurements_by_standarts_and_bs_codes.get_mut(&standart_str)
                            {
                                if let Some(ms) = bs_codes_measurements.get_mut(&bs_codes) {
                                    ms.push(*m);
                                } else {
                                    let mut ms = Vec::new();
                                    ms.push(*m);
                                    bs_codes_measurements.insert(bs_codes.clone(), ms);
                                }
                            }
                            set_codes.insert(bs_codes);
                        });
                    }
                });
            }
            Standart::WCDMA => {
                filtered_measurement = measurements
                    .iter()
                    .filter(|m| m.wcdma.is_some())
                    .collect::<Vec<_>>();
                if filtered_measurement.len() > 0 {
                    let bs_codes_measurements = HashMap::new();
                    measurements_by_standarts_and_bs_codes.insert(
                        standart_str.clone(),
                        bs_codes_measurements
                    );
                }
                filtered_measurement.iter().for_each(|m| {
                    if let Some(wcdma) = m.wcdma.as_ref() {
                        wcdma.iter().for_each(|wcdma_m| {
                            let bs_codes = BSCodes {
                                mcc: wcdma_m.mcc,
                                mnc: wcdma_m.mnc,
                                lac: wcdma_m.lac,
                                cid: wcdma_m.cid,
                            };
                            if
                                let Some(bs_codes_measurements) =
                                    measurements_by_standarts_and_bs_codes.get_mut(&standart_str)
                            {
                                if let Some(ms) = bs_codes_measurements.get_mut(&bs_codes) {
                                    ms.push(*m);
                                } else {
                                    let mut ms = Vec::new();
                                    ms.push(*m);
                                    bs_codes_measurements.insert(bs_codes.clone(), ms);
                                }
                            }
                            set_codes.insert(bs_codes);
                        });
                    }
                });
            }
            Standart::LTE => {
                filtered_measurement = measurements
                    .iter()
                    .filter(|m| m.lte.is_some())
                    .collect::<Vec<_>>();
                if filtered_measurement.len() > 0 {
                    let bs_codes_measurements = HashMap::new();
                    measurements_by_standarts_and_bs_codes.insert(
                        standart_str.clone(),
                        bs_codes_measurements
                    );
                }
                filtered_measurement.iter().for_each(|m| {
                    if let Some(lte) = m.lte.as_ref() {
                        lte.iter().for_each(|lte_m| {
                            let bs_codes = BSCodes {
                                mcc: lte_m.mcc,
                                mnc: lte_m.mnc,
                                lac: lte_m.lac,
                                cid: lte_m.cid,
                            };
                            if
                                let Some(bs_codes_measurements) =
                                    measurements_by_standarts_and_bs_codes.get_mut(&standart_str)
                            {
                                if let Some(ms) = bs_codes_measurements.get_mut(&bs_codes) {
                                    ms.push(*m);
                                } else {
                                    let mut ms = Vec::new();
                                    ms.push(*m);
                                    bs_codes_measurements.insert(bs_codes.clone(), ms);
                                }
                            }
                            set_codes.insert(bs_codes);
                        });
                    }
                });
            }
            Standart::FiveGNR => {
                filtered_measurement = measurements
                    .iter()
                    .filter(|m| m.five_gnr.is_some())
                    .collect::<Vec<_>>();
                if filtered_measurement.len() > 0 {
                    let bs_codes_measurements = HashMap::new();
                    measurements_by_standarts_and_bs_codes.insert(
                        standart_str.clone(),
                        bs_codes_measurements
                    );
                }
                filtered_measurement.iter().for_each(|m| {
                    if let Some(five_gnr) = m.five_gnr.as_ref() {
                        five_gnr.iter().for_each(|five_gnr_m| {
                            let bs_codes = BSCodes {
                                mcc: five_gnr_m.mcc,
                                mnc: five_gnr_m.mnc,
                                lac: five_gnr_m.lac,
                                cid: five_gnr_m.cid,
                            };
                            if
                                let Some(bs_codes_measurements) =
                                    measurements_by_standarts_and_bs_codes.get_mut(&standart_str)
                            {
                                if let Some(ms) = bs_codes_measurements.get_mut(&bs_codes) {
                                    ms.push(*m);
                                } else {
                                    let mut ms = Vec::new();
                                    ms.push(*m);
                                    bs_codes_measurements.insert(bs_codes.clone(), ms);
                                }
                            }
                            set_codes.insert(bs_codes);
                        });
                    }
                });
            }
        }
        if filtered_measurement.len() > 0 {
            measurements_by_standarts.insert(standart_str.clone(), filtered_measurement);
            bs_codes_by_standarts.insert(standart_str, set_codes);
        }
    }

    MeasurementsByStandarts {
        _measurements_by_standarts: measurements_by_standarts,
        bs_codes_by_standarts,
        measurements_by_standarts_and_bs_codes,
    }
}

// distribute measurements into partitions tables
pub fn distribute_points_by_partitions<'a>(
    measurements: &'a [Measurement],
    statistic_points: StatisticPoints,
    len_sp: usize
) -> HashMap<String, Vec<&'a Measurement>> {
    let mut dm: HashMap<String, Vec<&Measurement>> = HashMap::new();
    let default_partition = statistic_points.statistics
        .last()
        .map(|stat| &stat.partition)
        .unwrap();
    for m in measurements {
        let timestamp_m = m.timestamp.timestamp() as u64;
        let target_partition =
            // TODO: work out the default partition
            search_target_points_partition(timestamp_m, &statistic_points, len_sp).unwrap_or(
                default_partition
            );
        if let Some(ms) = dm.get_mut(target_partition) {
            ms.push(m);
        } else {
            let mut ms = Vec::with_capacity(measurements.len());
            ms.push(m);
            dm.insert(target_partition.clone(), ms);
        }
    }

    dm
}

pub async fn save_points_by_partitions(
    config: &Config,
    dm: HashMap<String, Vec<&Measurement>>,
    store_path: &str,
    points_schema: Arc<Schema>
) -> Result<(), anyhow::Error> {
    let mut handles = Vec::with_capacity(dm.capacity());
    let conn = connect(&format!("{}/points", store_path)).execute().await?;
    for (partition, part_measurements) in dm {
        let table = conn.open_table(&partition).execute().await?;

        let capacity = part_measurements.len();
        let mut vec_id = Vec::with_capacity(capacity);
        let mut vec_unit_id = Vec::with_capacity(capacity);
        let mut vec_timestamp = Vec::with_capacity(capacity);
        let mut vec_longitude = Vec::with_capacity(capacity);
        let mut vec_latitude = Vec::with_capacity(capacity);
        let mut vec_x = Vec::with_capacity(capacity);
        let mut vec_y = Vec::with_capacity(capacity);
        let mut vec_height_agl = Vec::with_capacity(capacity);
        let mut vec_height_asl = Vec::with_capacity(capacity);
        let mut base_stations = Vec::with_capacity(capacity);

        part_measurements.iter().for_each(|m| {
            vec_id.push(m.id);
            vec_unit_id.push(m.unit_id.as_str());
            vec_timestamp.push(m.timestamp.timestamp() as u64);
            // the estimate of the location occurs earlier
            // so we expect an Some variant
            if let Some(ref loc) = m.location {
                vec_longitude.push(loc.longitude);
                vec_latitude.push(loc.latitude);
                vec_x.push(loc.x);
                vec_y.push(loc.y);
                vec_height_agl.push(loc.height_agl);
                vec_height_asl.push(loc.height_asl);
            }
            // List of base_stations
            let mut bs = vec![];
            if let Some(ref gsm) = m.gsm {
                for gsm_m in gsm {
                    let bs_str = format!(
                        "gsm_{}_{}_{}_{}",
                        gsm_m.mcc,
                        gsm_m.mnc,
                        gsm_m.lac,
                        gsm_m.cid
                    );
                    bs.push(bs_str);
                }
            }
            if let Some(ref wcdma) = m.wcdma {
                for wcdma_m in wcdma {
                    let bs_str = format!(
                        "wcdma_{}_{}_{}_{}",
                        wcdma_m.mcc,
                        wcdma_m.mnc,
                        wcdma_m.lac,
                        wcdma_m.cid
                    );
                    bs.push(bs_str);
                }
            }
            if let Some(ref lte) = m.lte {
                for lte_m in lte {
                    let bs_str = format!(
                        "lte_{}_{}_{}_{}",
                        lte_m.mcc,
                        lte_m.mnc,
                        lte_m.lac,
                        lte_m.cid
                    );
                    bs.push(bs_str);
                }
            }
            if let Some(ref five_gnr) = m.five_gnr {
                for five_gnr_m in five_gnr {
                    let bs_str = format!(
                        "5gnr_{}_{}_{}_{}",
                        five_gnr_m.mcc,
                        five_gnr_m.mnc,
                        five_gnr_m.lac,
                        five_gnr_m.cid
                    );
                    bs.push(bs_str);
                }
            }
            base_stations.push(bs);
        });

        let array_id = Arc::new(UInt32Array::from(vec_id));
        let array_unit_id = Arc::new(StringArray::from(vec_unit_id));
        let array_timestamp = Arc::new(UInt64Array::from(vec_timestamp));
        let array_longitude = Arc::new(Float64Array::from(vec_longitude));
        let array_latitude = Arc::new(Float64Array::from(vec_latitude));
        let array_x = Arc::new(Float64Array::from(vec_x));
        let array_y = Arc::new(Float64Array::from(vec_y));
        let array_height_agl = Arc::new(Float64Array::from(vec_height_agl));
        let array_height_asl = Arc::new(Float64Array::from(vec_height_asl));
        let array_base_stations = Arc::new(base_stations_list(base_stations));

        let batch = RecordBatch::try_new(
            points_schema.clone(),
            vec![
                array_id,
                array_unit_id,
                array_timestamp,
                array_longitude,
                array_latitude,
                array_x,
                array_y,
                array_height_agl,
                array_height_asl,
                array_base_stations
            ]
        )?;

        let batches = RecordBatchIterator::new([Ok(batch)], points_schema.clone());
        // Define write parameters (e.g. overwrite dataset)
        let write_params = WriteParams {
            mode: WriteMode::Append,
            enable_move_stable_row_ids: true,
            ..Default::default()
        };

        let jh = tokio::spawn({
            let optimize_table = config.lancedb.optimize_table;
            async move {
                table
                    .add(batches)
                    .mode(AddDataMode::Append)
                    .write_options(WriteOptions {
                        lance_write_params: Some(write_params),
                    })
                    .execute().await?;

                if optimize_table {
                    table.optimize(OptimizeAction::Compact {
                        options: CompactionOptions {
                            target_rows_per_fragment: 2 * 1024 * 1024,
                            batch_size: Some(2 * 1024 * 1024),
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
                }
                Ok::<_, anyhow::Error>(())
            }
        });
        handles.push(jh);
    }

    for handle in handles {
        if let Err(err) = handle.await {
            event!(Level::ERROR, "Error save points by partitions from task: {err}");
        }
    }

    Ok(())
}

fn search_target_points_partition(
    ts: u64,
    statistic_points: &StatisticPoints,
    len_sp: usize
) -> Result<&String, anyhow::Error> {
    for (i, stat) in statistic_points.statistics.iter().enumerate() {
        let partition = &stat.partition;
        if let Some(start) = stat.start {
            if let Some(end) = stat.end {
                // write in a closed partition table as needed
                if ts >= start && ts <= end {
                    return Ok(partition);
                }
            } else if ts >= start && i == len_sp - 1 {
                // write to the currently open table
                return Ok(partition);
            }
        }
    }

    Err(anyhow::anyhow!("Error search partition points table by timestamp {ts}"))
}

fn base_stations_list(base_stations_vec: Vec<Vec<String>>) -> GenericListArray<i32> {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for base_stations in base_stations_vec {
        if base_stations.len() > 0 {
            // append array, for example ["gsm_250_99_4556_98655"]
            for bs in base_stations {
                builder.values().append_value(bs);
            }
            builder.append(true);
        } else {
            // append NULL
            builder.append(false);
        }
    }
    builder.finish()
}
