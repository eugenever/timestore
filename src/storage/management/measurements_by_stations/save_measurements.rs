use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    ArrayRef,
    RecordBatch,
    RecordBatchIterator,
    Float64Array,
    StringArray,
    UInt32Array,
    UInt64Array,
};
use arrow_schema::Schema;
use lance::dataset::{ WriteMode, WriteParams };
use lancedb::table::{ AddDataMode, CompactionOptions, OptimizeAction, WriteOptions };
use lancedb::{ connect, Table };
use tracing::{ event, Level };
use tokio::task::JoinHandle;
use tokio::sync::oneshot;

use crate::config::Config;
use crate::storage::{ models::{ BSCodes, Measurement }, schema::Standart };

use super::{
    StatisticsPartitionMeasurementsByStation,
    MessageStatisticsPartitionMeasurementsByStation,
    database_station_directory_from_codes,
};

async fn save_batch_and_optimize_table(
    config: &Config,
    table: &Table,
    batch: RecordBatch,
    schema: Arc<Schema>
) -> Result<(), anyhow::Error> {
    let batches = RecordBatchIterator::new([Ok(batch)], schema.clone());
    // Define write parameters (e.g. overwrite dataset)
    let write_params = WriteParams {
        mode: WriteMode::Append,
        enable_move_stable_row_ids: true,
        ..Default::default()
    };
    table
        .add(batches)
        .mode(AddDataMode::Append)
        .write_options(WriteOptions {
            lance_write_params: Some(write_params),
        })
        .execute().await?;

    if config.lancedb.optimize_table {
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

    Ok(())
}

pub async fn save_gsm_measurements_by_partitions(
    config: &Config,
    dm: HashMap<String, Vec<&Measurement>>,
    store_path: &str,
    bs_codes: &BSCodes,
    schema: Arc<Schema>,
    tx_spm: flume::Sender<MessageStatisticsPartitionMeasurementsByStation>
) -> Result<Vec<JoinHandle<Result<(), anyhow::Error>>>, anyhow::Error> {
    let mut handles = Vec::with_capacity(dm.capacity());
    let (_, db_directory) = database_station_directory_from_codes(
        store_path,
        Standart::GSM.as_ref(),
        bs_codes
    );
    let conn = connect(&db_directory).execute().await?;
    for (partition, part_measurements) in dm {
        // Calculation of the number of measurements by GSM
        let mut capacity = 0;
        part_measurements.iter().for_each(|m| {
            if let Some(ref gsm_ms) = m.gsm {
                capacity = capacity + gsm_ms.len();
            }
        });

        let table = conn.open_table(&partition).execute().await?;

        let mut temp_buffers = temporary_buffers(capacity);
        // unique buffers
        let mut vec_bsic = Vec::with_capacity(capacity);
        let mut vec_arfcn = Vec::with_capacity(capacity);

        part_measurements.iter().for_each(|m| {
            if let Some(ref gsm_ms) = m.gsm {
                let filtered_gsm_ms = gsm_ms
                    .iter()
                    .filter(|temp_gsm_m| {
                        temp_gsm_m.mcc == bs_codes.mcc &&
                            temp_gsm_m.mnc == bs_codes.mnc &&
                            temp_gsm_m.lac == bs_codes.lac &&
                            temp_gsm_m.cid == bs_codes.cid
                    })
                    .collect::<Vec<_>>();
                for gsm_m in filtered_gsm_ms {
                    temp_buffers.id.push(m.id);
                    temp_buffers.unit_id.push(m.unit_id.as_str());
                    temp_buffers.timestamp.push(m.timestamp.timestamp() as u64);
                    // the estimate of the location occurs earlier
                    // so we expect an Some variant
                    if let Some(ref loc) = m.location {
                        temp_buffers.longitude.push(loc.longitude);
                        temp_buffers.latitude.push(loc.latitude);
                        temp_buffers.x.push(loc.x);
                        temp_buffers.y.push(loc.y);
                        temp_buffers.height_agl.push(loc.height_agl);
                        temp_buffers.height_asl.push(loc.height_asl);
                    }

                    temp_buffers.mcc.push(gsm_m.mcc);
                    temp_buffers.mnc.push(gsm_m.mnc);
                    temp_buffers.lac.push(gsm_m.lac);
                    temp_buffers.cid.push(gsm_m.cid);

                    temp_buffers.power.push(gsm_m.power);
                    temp_buffers.delay.push(gsm_m.delay);

                    vec_bsic.push(gsm_m.bsic);
                    vec_arfcn.push(gsm_m.arfcn);
                }
            }
        });

        let sb = StandartBuffers::GSM { bsic: vec_bsic, arfcn: vec_arfcn, schema: schema.clone() };
        let batch = record_batch_from_buffers(temp_buffers, sb)?;
        let jh = spawn_save_task(
            config.clone(),
            table,
            bs_codes.clone(),
            schema.clone(),
            batch,
            partition,
            tx_spm.clone(),
            Standart::GSM.to_string()
        );
        handles.push(jh);
    }

    Ok(handles)
}

pub async fn save_wcdma_measurements_by_partitions(
    config: &Config,
    dm: HashMap<String, Vec<&Measurement>>,
    store_path: &str,
    bs_codes: &BSCodes,
    schema: Arc<Schema>,
    tx_spm: flume::Sender<MessageStatisticsPartitionMeasurementsByStation>
) -> Result<Vec<JoinHandle<Result<(), anyhow::Error>>>, anyhow::Error> {
    let mut handles = Vec::with_capacity(dm.capacity());
    let (_, db_directory) = database_station_directory_from_codes(
        store_path,
        Standart::WCDMA.as_ref(),
        bs_codes
    );
    let conn = connect(&db_directory).execute().await?;
    for (partition, part_measurements) in dm {
        // Calculation of the number of measurements by WCDMA
        let mut capacity = 0;
        part_measurements.iter().for_each(|m| {
            if let Some(ref wcdma_ms) = m.wcdma {
                capacity = capacity + wcdma_ms.len();
            }
        });

        let table = conn.open_table(&partition).execute().await?;

        let mut temp_buffers = temporary_buffers(capacity);
        // unique buffers
        let mut vec_psc = Vec::with_capacity(capacity);
        let mut vec_uarfcn = Vec::with_capacity(capacity);

        part_measurements.iter().for_each(|m| {
            if let Some(ref wcdma_ms) = m.wcdma {
                let filtered_wcdma_ms = wcdma_ms
                    .iter()
                    .filter(|temp_wcdma_m| {
                        temp_wcdma_m.mcc == bs_codes.mcc &&
                            temp_wcdma_m.mnc == bs_codes.mnc &&
                            temp_wcdma_m.lac == bs_codes.lac &&
                            temp_wcdma_m.cid == bs_codes.cid
                    })
                    .collect::<Vec<_>>();
                for wcdma_m in filtered_wcdma_ms {
                    temp_buffers.id.push(m.id);
                    temp_buffers.unit_id.push(m.unit_id.as_str());
                    temp_buffers.timestamp.push(m.timestamp.timestamp() as u64);
                    // the estimate of the location occurs earlier
                    // so we expect an Some variant
                    if let Some(ref loc) = m.location {
                        temp_buffers.longitude.push(loc.longitude);
                        temp_buffers.latitude.push(loc.latitude);
                        temp_buffers.x.push(loc.x);
                        temp_buffers.y.push(loc.y);
                        temp_buffers.height_agl.push(loc.height_agl);
                        temp_buffers.height_asl.push(loc.height_asl);
                    }

                    temp_buffers.mcc.push(wcdma_m.mcc);
                    temp_buffers.mnc.push(wcdma_m.mnc);
                    temp_buffers.lac.push(wcdma_m.lac);
                    temp_buffers.cid.push(wcdma_m.cid);

                    temp_buffers.power.push(wcdma_m.power);
                    temp_buffers.delay.push(wcdma_m.delay);

                    vec_psc.push(wcdma_m.psc);
                    vec_uarfcn.push(wcdma_m.uarfcn);
                }
            }
        });

        let sb = StandartBuffers::WCDMA {
            psc: vec_psc,
            uarfcn: vec_uarfcn,
            schema: schema.clone(),
        };
        let batch = record_batch_from_buffers(temp_buffers, sb)?;
        let jh = spawn_save_task(
            config.clone(),
            table,
            bs_codes.clone(),
            schema.clone(),
            batch,
            partition,
            tx_spm.clone(),
            Standart::WCDMA.to_string()
        );
        handles.push(jh);
    }

    Ok(handles)
}

pub async fn save_lte_measurements_by_partitions(
    config: &Config,
    dm: HashMap<String, Vec<&Measurement>>,
    store_path: &str,
    bs_codes: &BSCodes,
    schema: Arc<Schema>,
    tx_spm: flume::Sender<MessageStatisticsPartitionMeasurementsByStation>
) -> Result<Vec<JoinHandle<Result<(), anyhow::Error>>>, anyhow::Error> {
    let mut handles = Vec::with_capacity(dm.capacity());
    let (_, db_directory) = database_station_directory_from_codes(
        store_path,
        Standart::LTE.as_ref(),
        bs_codes
    );
    let conn = connect(&db_directory).execute().await?;
    for (partition, part_measurements) in dm {
        // Calculation of the number of measurements by LTE
        let mut capacity = 0;
        part_measurements.iter().for_each(|m| {
            if let Some(ref lte_ms) = m.lte {
                capacity = capacity + lte_ms.len();
            }
        });

        let table = conn.open_table(&partition).execute().await?;

        let mut temp_buffers = temporary_buffers(capacity);
        // unique buffers
        let mut vec_pci = Vec::with_capacity(capacity);
        let mut vec_earfcn = Vec::with_capacity(capacity);

        part_measurements.iter().for_each(|m| {
            if let Some(ref lte_ms) = m.lte {
                let filtered_lte_ms = lte_ms
                    .iter()
                    .filter(|temp_lte_ms| {
                        temp_lte_ms.mcc == bs_codes.mcc &&
                            temp_lte_ms.mnc == bs_codes.mnc &&
                            temp_lte_ms.lac == bs_codes.lac &&
                            temp_lte_ms.cid == bs_codes.cid
                    })
                    .collect::<Vec<_>>();
                for lte_m in filtered_lte_ms {
                    temp_buffers.id.push(m.id);
                    temp_buffers.unit_id.push(m.unit_id.as_str());
                    temp_buffers.timestamp.push(m.timestamp.timestamp() as u64);
                    // the estimate of the location occurs earlier
                    // so we expect an Some variant
                    if let Some(ref loc) = m.location {
                        temp_buffers.longitude.push(loc.longitude);
                        temp_buffers.latitude.push(loc.latitude);
                        temp_buffers.x.push(loc.x);
                        temp_buffers.y.push(loc.y);
                        temp_buffers.height_agl.push(loc.height_agl);
                        temp_buffers.height_asl.push(loc.height_asl);
                    }

                    temp_buffers.mcc.push(lte_m.mcc);
                    temp_buffers.mnc.push(lte_m.mnc);
                    temp_buffers.lac.push(lte_m.lac);
                    temp_buffers.cid.push(lte_m.cid);

                    temp_buffers.power.push(lte_m.power);
                    temp_buffers.delay.push(lte_m.delay);

                    vec_pci.push(lte_m.pci);
                    vec_earfcn.push(lte_m.earfcn);
                }
            }
        });

        let sb = StandartBuffers::LTE { pci: vec_pci, earfcn: vec_earfcn, schema: schema.clone() };
        let batch = record_batch_from_buffers(temp_buffers, sb)?;
        let jh = spawn_save_task(
            config.clone(),
            table,
            bs_codes.clone(),
            schema.clone(),
            batch,
            partition,
            tx_spm.clone(),
            Standart::LTE.to_string()
        );
        handles.push(jh);
    }

    Ok(handles)
}

pub async fn save_five_gnr_measurements_by_partitions(
    config: &Config,
    dm: HashMap<String, Vec<&Measurement>>,
    store_path: &str,
    bs_codes: &BSCodes,
    schema: Arc<Schema>,
    tx_spm: flume::Sender<MessageStatisticsPartitionMeasurementsByStation>
) -> Result<Vec<JoinHandle<Result<(), anyhow::Error>>>, anyhow::Error> {
    let mut handles = Vec::with_capacity(dm.capacity());
    let (_, db_directory) = database_station_directory_from_codes(
        store_path,
        Standart::FiveGNR.as_ref(),
        bs_codes
    );
    let conn = connect(&db_directory).execute().await?;
    for (partition, part_measurements) in dm {
        // Calculation of the number of measurements by 5GNR
        let mut capacity = 0;
        part_measurements.iter().for_each(|m| {
            if let Some(ref five_gnr_ms) = m.five_gnr {
                capacity = capacity + five_gnr_ms.len();
            }
        });

        let table = conn.open_table(&partition).execute().await?;

        let mut temp_buffers = temporary_buffers(capacity);
        // unique buffers
        let mut vec_pci = Vec::with_capacity(capacity);
        let mut vec_earfcn = Vec::with_capacity(capacity);

        part_measurements.iter().for_each(|m| {
            if let Some(ref five_gnr_ms) = m.five_gnr {
                let filtered_five_gnr_ms = five_gnr_ms
                    .iter()
                    .filter(|temp_five_gnr_ms| {
                        temp_five_gnr_ms.mcc == bs_codes.mcc &&
                            temp_five_gnr_ms.mnc == bs_codes.mnc &&
                            temp_five_gnr_ms.lac == bs_codes.lac &&
                            temp_five_gnr_ms.cid == bs_codes.cid
                    })
                    .collect::<Vec<_>>();
                for five_gnr_m in filtered_five_gnr_ms {
                    temp_buffers.id.push(m.id);
                    temp_buffers.unit_id.push(m.unit_id.as_str());
                    temp_buffers.timestamp.push(m.timestamp.timestamp() as u64);
                    // the estimate of the location occurs earlier
                    // so we expect an Some variant
                    if let Some(ref loc) = m.location {
                        temp_buffers.longitude.push(loc.longitude);
                        temp_buffers.latitude.push(loc.latitude);
                        temp_buffers.x.push(loc.x);
                        temp_buffers.y.push(loc.y);
                        temp_buffers.height_agl.push(loc.height_agl);
                        temp_buffers.height_asl.push(loc.height_asl);
                    }

                    temp_buffers.mcc.push(five_gnr_m.mcc);
                    temp_buffers.mnc.push(five_gnr_m.mnc);
                    temp_buffers.lac.push(five_gnr_m.lac);
                    temp_buffers.cid.push(five_gnr_m.cid);

                    temp_buffers.power.push(five_gnr_m.power);
                    temp_buffers.delay.push(five_gnr_m.delay);

                    vec_pci.push(five_gnr_m.pci);
                    vec_earfcn.push(five_gnr_m.earfcn);
                }
            }
        });

        let sb = StandartBuffers::FiveGNR {
            pci: vec_pci,
            earfcn: vec_earfcn,
            schema: schema.clone(),
        };
        let batch = record_batch_from_buffers(temp_buffers, sb)?;
        let jh = spawn_save_task(
            config.clone(),
            table,
            bs_codes.clone(),
            schema.clone(),
            batch,
            partition,
            tx_spm.clone(),
            Standart::FiveGNR.to_string()
        );
        handles.push(jh);
    }

    Ok(handles)
}

fn spawn_save_task(
    config: Config,
    table: Table,
    bs_codes: BSCodes,
    schema: Arc<Schema>,
    batch: RecordBatch,
    partition: String,
    tx_spm: flume::Sender<MessageStatisticsPartitionMeasurementsByStation>,
    standart: String
) -> JoinHandle<Result<(), anyhow::Error>> {
    tokio::spawn({
        async move {
            save_batch_and_optimize_table(&config, &table, batch, schema.clone()).await?;
            // update metadata
            let count_rows = table.count_rows(None).await? as u64;
            let stat = StatisticsPartitionMeasurementsByStation {
                partition,
                count_rows,
                standart: Some(standart),
                mcc: Some(bs_codes.mcc),
                mnc: Some(bs_codes.mnc),
                lac: Some(bs_codes.lac),
                cid: Some(bs_codes.cid),
                ..Default::default()
            };

            let (tx_submit, rx_submit) = oneshot::channel::<()>();
            match
                tx_spm.send_async(
                    MessageStatisticsPartitionMeasurementsByStation::CreateUpdateBSStat {
                        updated_stat: stat,
                        tx_submit,
                    }
                ).await
            {
                Err(err) => {
                    event!(
                        Level::ERROR,
                        "Error send create/update station statistic message: {err}"
                    );
                }
                Ok(_) => {
                    if let Err(err) = rx_submit.await {
                        event!(Level::ERROR, "Error receive submit: {err}");
                    }
                }
            }

            Ok::<_, anyhow::Error>(())
        }
    })
}

pub struct Buffers<'a> {
    id: Vec<Option<u32>>,
    unit_id: Vec<&'a str>,
    timestamp: Vec<u64>,
    longitude: Vec<f64>,
    latitude: Vec<f64>,
    x: Vec<Option<f64>>,
    y: Vec<Option<f64>>,
    height_agl: Vec<f64>,
    height_asl: Vec<Option<f64>>,
    mcc: Vec<u32>,
    mnc: Vec<u32>,
    lac: Vec<u32>,
    cid: Vec<u32>,
    power: Vec<Option<f64>>,
    delay: Vec<Option<f64>>,
}

fn temporary_buffers<'a>(capacity: usize) -> Buffers<'a> {
    // base
    let id = Vec::with_capacity(capacity);
    let unit_id = Vec::with_capacity(capacity);
    let timestamp = Vec::with_capacity(capacity);
    let longitude = Vec::with_capacity(capacity);
    let latitude = Vec::with_capacity(capacity);
    let x = Vec::with_capacity(capacity);
    let y = Vec::with_capacity(capacity);
    let height_agl = Vec::with_capacity(capacity);
    let height_asl = Vec::with_capacity(capacity);
    // codes
    let mcc = Vec::with_capacity(capacity);
    let mnc = Vec::with_capacity(capacity);
    let lac = Vec::with_capacity(capacity);
    let cid = Vec::with_capacity(capacity);
    // common
    let power = Vec::with_capacity(capacity);
    let delay = Vec::with_capacity(capacity);

    Buffers {
        id,
        unit_id,
        timestamp,
        longitude,
        latitude,
        x,
        y,
        height_agl,
        height_asl,
        mcc,
        mnc,
        lac,
        cid,
        power,
        delay,
    }
}

pub enum StandartBuffers {
    GSM {
        bsic: Vec<Option<u32>>,
        arfcn: Vec<Option<u32>>,
        schema: Arc<Schema>,
    },
    WCDMA {
        psc: Vec<Option<u32>>,
        uarfcn: Vec<Option<u32>>,
        schema: Arc<Schema>,
    },
    LTE {
        pci: Vec<Option<u32>>,
        earfcn: Vec<Option<u32>>,
        schema: Arc<Schema>,
    },
    FiveGNR {
        pci: Vec<Option<u32>>,
        earfcn: Vec<Option<u32>>,
        schema: Arc<Schema>,
    },
}

pub fn record_batch_from_buffers<'a>(
    temp_buffers: Buffers<'a>,
    sb: StandartBuffers
) -> Result<RecordBatch, anyhow::Error> {
    let array_id = Arc::new(UInt32Array::from(temp_buffers.id));
    let array_unit_id = Arc::new(StringArray::from(temp_buffers.unit_id));
    let array_timestamp = Arc::new(UInt64Array::from(temp_buffers.timestamp));
    let array_longitude = Arc::new(Float64Array::from(temp_buffers.longitude));
    let array_latitude = Arc::new(Float64Array::from(temp_buffers.latitude));
    let array_x = Arc::new(Float64Array::from(temp_buffers.x));
    let array_y = Arc::new(Float64Array::from(temp_buffers.y));
    let array_height_agl = Arc::new(Float64Array::from(temp_buffers.height_agl));
    let array_height_asl = Arc::new(Float64Array::from(temp_buffers.height_asl));

    let array_mcc = Arc::new(UInt32Array::from(temp_buffers.mcc));
    let array_mnc = Arc::new(UInt32Array::from(temp_buffers.mnc));
    let array_lac = Arc::new(UInt32Array::from(temp_buffers.lac));
    let array_cid = Arc::new(UInt32Array::from(temp_buffers.cid));

    let array_power = Arc::new(Float64Array::from(temp_buffers.power));
    let array_delay = Arc::new(Float64Array::from(temp_buffers.delay));

    let mut columns: Vec<ArrayRef> = vec![
        array_id,
        array_unit_id,
        array_timestamp,
        array_longitude,
        array_latitude,
        array_x,
        array_y,
        array_height_agl,
        array_height_asl,
        array_mcc,
        array_mnc,
        array_lac,
        array_cid
    ];

    match sb {
        StandartBuffers::GSM { bsic, arfcn, schema } => {
            let array_bsic = Arc::new(UInt32Array::from(bsic));
            let array_arfcn = Arc::new(UInt32Array::from(arfcn));
            let specific_arrays: Vec<ArrayRef> = vec![
                array_bsic,
                array_arfcn,
                array_power,
                array_delay
            ];
            columns.extend(specific_arrays);
            Ok(RecordBatch::try_new(schema.clone(), columns)?)
        }
        StandartBuffers::WCDMA { psc, uarfcn, schema } => {
            let array_psc = Arc::new(UInt32Array::from(psc));
            let array_uarfcn = Arc::new(UInt32Array::from(uarfcn));
            let specific_arrays: Vec<ArrayRef> = vec![
                array_psc,
                array_uarfcn,
                array_power,
                array_delay
            ];
            columns.extend(specific_arrays);
            Ok(RecordBatch::try_new(schema.clone(), columns)?)
        }
        StandartBuffers::LTE { pci, earfcn, schema } => {
            let array_pci = Arc::new(UInt32Array::from(pci));
            let array_earfcn = Arc::new(UInt32Array::from(earfcn));
            let specific_arrays: Vec<ArrayRef> = vec![
                array_pci,
                array_earfcn,
                array_power,
                array_delay
            ];
            columns.extend(specific_arrays);
            Ok(RecordBatch::try_new(schema.clone(), columns)?)
        }
        StandartBuffers::FiveGNR { pci, earfcn, schema } => {
            let array_pci = Arc::new(UInt32Array::from(pci));
            let array_earfcn = Arc::new(UInt32Array::from(earfcn));
            let specific_arrays: Vec<ArrayRef> = vec![
                array_pci,
                array_earfcn,
                array_power,
                array_delay
            ];
            columns.extend(specific_arrays);
            Ok(RecordBatch::try_new(schema.clone(), columns)?)
        }
    }
}
