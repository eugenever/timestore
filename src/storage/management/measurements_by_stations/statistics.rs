use lancedb::Table;
use polars::datatypes::AnyValue;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{ event, Level };
use itertools::izip;

use crate::storage::models::BSCodes;

use super::{ MessageMetadataMeasurementsByStation, load_measurements_by_station_metadata_table };

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

#[allow(unused)]
impl StatisticsPartitionMeasurementsByStation {
    fn from_polars_dataframe_row(row: &[AnyValue<'_>]) -> Self {
        // check the columns that may be NULL/None separately
        let id = match row[0].try_extract() {
            Ok(v) => Some(v),
            Err(_) => None,
        };
        let start = match row[2].try_extract() {
            Ok(v) => Some(v),
            Err(_) => None,
        };
        let end = match row[3].try_extract() {
            Ok(v) => Some(v),
            Err(_) => None,
        };
        let standart = match row[5].get_str() {
            Some(v) => Some(v.to_string()),
            None => None,
        };
        let mcc = match row[6].try_extract() {
            Ok(v) => Some(v),
            Err(_) => None,
        };
        let mnc = match row[7].try_extract() {
            Ok(v) => Some(v),
            Err(_) => None,
        };
        let lac = match row[8].try_extract() {
            Ok(v) => Some(v),
            Err(_) => None,
        };
        let cid = match row[9].try_extract() {
            Ok(v) => Some(v),
            Err(_) => None,
        };

        Self {
            id,
            partition: row[1].get_str().unwrap().to_string(),
            start,
            end,
            count_rows: row[4].try_extract().unwrap(),
            standart,
            mcc,
            mnc,
            lac,
            cid,
        }
    }
}

pub enum MessageStatisticsPartitionMeasurementsByStation {
    GetAllBSStats {
        standart: String,
        bs_codes: BSCodes,
        tx_stats: oneshot::Sender<Vec<StatisticsPartitionMeasurementsByStation>>,
    },
    CreateUpdateBSStat {
        updated_stat: StatisticsPartitionMeasurementsByStation,
        tx_submit: oneshot::Sender<()>,
    },
}

pub fn statistics_partition_measurements_by_station_task(
    measurements_metadata_table: Table,
    rx: flume::Receiver<MessageStatisticsPartitionMeasurementsByStation>,
    tx_umss: flume::Sender<MessageMetadataMeasurementsByStation>
) -> Result<JoinHandle<Result<(), anyhow::Error>>, anyhow::Error> {
    let jh = tokio::spawn({
        async move {
            // load once at startup
            let df_mmt = load_measurements_by_station_metadata_table(
                &measurements_metadata_table
            ).await?;

            // transform the dataframe into a vector of statistics
            let capacity = df_mmt.column("id")?.len();
            let mut statistics: Vec<StatisticsPartitionMeasurementsByStation> =
                Vec::with_capacity(capacity);
            for (id, partition, start, end, count_rows, standart, mcc, mnc, lac, cid) in izip!(
                df_mmt.column("id")?.u32()?.into_iter(),
                df_mmt.column("partition")?.str()?.into_iter(),
                df_mmt.column("start")?.u64()?.into_iter(),
                df_mmt.column("end")?.u64()?.into_iter(),
                df_mmt.column("count_rows")?.u64()?.into_iter(),
                df_mmt.column("standart")?.str()?.into_iter(),
                df_mmt.column("mcc")?.u32()?.into_iter(),
                df_mmt.column("mnc")?.u32()?.into_iter(),
                df_mmt.column("lac")?.u32()?.into_iter(),
                df_mmt.column("cid")?.u32()?.into_iter()
            ) {
                statistics.push(StatisticsPartitionMeasurementsByStation {
                    id,
                    partition: partition.unwrap().to_string(),
                    start,
                    end,
                    count_rows: count_rows.unwrap(),
                    standart: standart.map(|s| s.to_string()),
                    mcc,
                    mnc,
                    lac,
                    cid,
                });
            }

            while let Ok(message) = rx.recv_async().await {
                match message {
                    MessageStatisticsPartitionMeasurementsByStation::GetAllBSStats {
                        standart,
                        bs_codes,
                        tx_stats,
                    } => {
                        let mut bs_stats = statistics
                            .iter()
                            .filter(
                                |stat|
                                    *stat.standart.as_ref().unwrap() == standart &&
                                    *stat.mcc.as_ref().unwrap() == bs_codes.mcc &&
                                    *stat.mnc.as_ref().unwrap() == bs_codes.mnc &&
                                    *stat.lac.as_ref().unwrap() == bs_codes.lac &&
                                    *stat.cid.as_ref().unwrap() == bs_codes.cid
                            )
                            .cloned()
                            .collect::<Vec<_>>();

                        bs_stats.sort_by_key(|stat| {
                            let split_partition = stat.partition.split('_').collect::<Vec<&str>>();
                            let partition_id = split_partition[1].parse::<u32>().unwrap();
                            partition_id
                        });

                        if let Err(_err) = tx_stats.send(bs_stats) {
                            event!(
                                Level::ERROR,
                                "Error send statistics for standart '{standart}'and station codes {bs_codes:?}"
                            );
                        }
                    }
                    MessageStatisticsPartitionMeasurementsByStation::CreateUpdateBSStat {
                        updated_stat,
                        tx_submit,
                    } => {
                        if
                            let Some(old_stat) = statistics
                                .iter_mut()
                                .find(
                                    |stat|
                                        *stat.partition == updated_stat.partition &&
                                        stat.standart == updated_stat.standart &&
                                        stat.mcc == updated_stat.mcc &&
                                        stat.mnc == updated_stat.mnc &&
                                        stat.lac == updated_stat.lac &&
                                        stat.cid == updated_stat.cid
                                )
                        {
                            // update case
                            update_statistic_measurements_by_stations(old_stat, &updated_stat);
                            // update metadata table without waiting => tx_submit: None
                            if
                                let Err(err) = tx_umss.send_async(
                                    MessageMetadataMeasurementsByStation::Update {
                                        measurements_metadata_table: measurements_metadata_table.clone(),
                                        stat: updated_stat,
                                        tx_submit: None,
                                    }
                                ).await
                            {
                                event!(
                                    Level::ERROR,
                                    "Error send update metadata measurements of station message: {err}"
                                );
                            }
                        } else {
                            // create case
                            statistics.push(updated_stat);
                        }
                        if let Err(_err) = tx_submit.send(()) {
                            event!(
                                Level::ERROR,
                                "Error send submit on create/update station statistic message"
                            );
                        }
                    }
                }
            }
            Ok::<_, anyhow::Error>(())
        }
    });

    Ok(jh)
}

pub fn update_statistic_measurements_by_stations(
    old_stat: &mut StatisticsPartitionMeasurementsByStation,
    updated_stat: &StatisticsPartitionMeasurementsByStation
) {
    if updated_stat.id.is_some() {
        old_stat.id = updated_stat.id;
    }
    if updated_stat.start.is_some() {
        old_stat.start = updated_stat.start;
    }
    if updated_stat.end.is_some() {
        old_stat.end = updated_stat.end;
    }
    old_stat.count_rows = updated_stat.count_rows;
    if updated_stat.standart.is_some() {
        old_stat.standart = updated_stat.standart.clone();
    }
    if updated_stat.mcc.is_some() {
        old_stat.mcc = updated_stat.mcc;
    }
    if updated_stat.mnc.is_some() {
        old_stat.mnc = updated_stat.mnc;
    }
    if updated_stat.lac.is_some() {
        old_stat.lac = updated_stat.lac;
    }
    if updated_stat.cid.is_some() {
        old_stat.cid = updated_stat.cid;
    }
}

pub async fn send_update_message(
    tx_spm: flume::Sender<MessageStatisticsPartitionMeasurementsByStation>,
    stat: StatisticsPartitionMeasurementsByStation
) -> Result<(), anyhow::Error> {
    let (tx_submit, rx_submit) = oneshot::channel::<()>();
    match
        tx_spm.send_async(MessageStatisticsPartitionMeasurementsByStation::CreateUpdateBSStat {
            updated_stat: stat,
            tx_submit,
        }).await
    {
        Err(err) => {
            event!(Level::ERROR, "Error send create/update station statistic message: {err}");
        }
        Ok(_) => {
            if let Err(err) = rx_submit.await {
                event!(Level::ERROR, "Error receive submit create/update station statistic: {err}");
            }
        }
    }

    Ok(())
}

pub async fn send_get_bs_stats_message(
    tx_spm: flume::Sender<MessageStatisticsPartitionMeasurementsByStation>,
    standart: &str,
    bs_codes: &BSCodes
) -> Result<Vec<StatisticsPartitionMeasurementsByStation>, anyhow::Error> {
    let (tx_stats, rx_stats) = oneshot::channel();
    match
        tx_spm.send_async(MessageStatisticsPartitionMeasurementsByStation::GetAllBSStats {
            standart: standart.to_string(),
            bs_codes: bs_codes.clone(),
            tx_stats,
        }).await
    {
        Err(err) => {
            let err = format!("Error send all statistics of station message: {err}");
            event!(Level::ERROR, "{}", err.clone());
            return Err(anyhow::anyhow!(err));
        }
        Ok(_) => {
            match rx_stats.await {
                Err(err) => {
                    let err = format!("Error receive all statistics of station message: {err}");
                    event!(Level::ERROR, "Error receive all statistics of station message: {err}");
                    return Err(anyhow::anyhow!(err));
                }
                Ok(stats) => {
                    return Ok(stats);
                }
            }
        }
    }
}
