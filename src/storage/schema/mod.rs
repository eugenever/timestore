#![allow(unused)]
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use arrow::datatypes::{ DataType, Field, Schema };
use strum_macros::{ AsRefStr, Display, EnumCount, EnumIter };

#[derive(Debug, AsRefStr, EnumIter, EnumCount, Display, PartialEq, Clone)]
pub enum Standart {
    #[strum(serialize = "GSM", to_string = "GSM")]
    GSM,
    #[strum(serialize = "WCDMA", to_string = "WCDMA")]
    WCDMA,
    #[strum(serialize = "LTE", to_string = "LTE")]
    LTE,
    #[strum(serialize = "5GNR", to_string = "5GNR")]
    FiveGNR,
}

impl FromStr for Standart {
    type Err = anyhow::Error;
    fn from_str(input: &str) -> Result<Standart, Self::Err> {
        match input {
            "GSM" => Ok(Standart::GSM),
            "WCDMA" => Ok(Standart::WCDMA),
            "LTE" => Ok(Standart::LTE),
            "5GNR" => Ok(Standart::FiveGNR),
            _ => Err(anyhow!("Error parse Standart from '{input}'")),
        }
    }
}

pub fn get_schema_by_standart(standart: &Standart) -> Result<Arc<Schema>, anyhow::Error> {
    let s = match standart {
        Standart::GSM => Arc::new(gsm_measurement_schema()?),
        Standart::WCDMA => Arc::new(wcdma_measurement_schema()?),
        Standart::LTE => Arc::new(lte_measurement_schema()?),
        Standart::FiveGNR => Arc::new(five_gnr_measurement_schema()?),
    };
    Ok(s)
}

pub fn get_points_metadata_schema() -> Result<Schema, anyhow::Error> {
    let bs = base_metadata_schema();
    Schema::try_merge(vec![bs]).map_err(|err| anyhow::anyhow!(err))
}

pub fn get_measurements_metadata_schema() -> Result<Schema, anyhow::Error> {
    let bs = base_metadata_schema();
    let cs = codes_schema();
    Schema::try_merge(
        vec![bs, Schema::new(vec![Field::new("standart", DataType::Utf8, true)]), cs]
    ).map_err(|err| anyhow::anyhow!(err))
}

pub fn base_metadata_schema() -> Schema {
    Schema::new(
        vec![
            Field::new("id", DataType::UInt32, true),
            // Name of LanceDB partition table
            Field::new("partition", DataType::Utf8, false),
            // start, end timestamps in seconds
            Field::new("start", DataType::UInt64, true),
            Field::new("end", DataType::UInt64, true),
            Field::new("count_rows", DataType::UInt64, false),
            // if moved to remote storage
            Field::new("remote_store", DataType::Utf8, true)
        ]
    )
}

pub fn points_schema() -> Schema {
    Schema::new(
        vec![
            Field::new("id", DataType::UInt32, true),
            Field::new("unit_id", DataType::Utf8, true),
            // timestamp in seconds
            Field::new("timestamp", DataType::UInt64, false),
            // Location
            Field::new("longitude", DataType::Float64, false),
            Field::new("latitude", DataType::Float64, false),
            // Longitude
            Field::new("x", DataType::Float64, true),
            // Latitude
            Field::new("y", DataType::Float64, true),
            Field::new("height_agl", DataType::Float64, true),
            Field::new("height_asl", DataType::Float64, true)
        ]
    )
}

pub fn get_extended_points_schema() -> Result<Schema, anyhow::Error> {
    let ps = points_schema();
    Schema::try_merge(
        vec![
            ps,
            Schema::new(
                vec![
                    // List of base stations
                    Field::new(
                        "base_stations",
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                        true
                    )
                ]
            )
        ]
    ).map_err(|err| anyhow::anyhow!(err))
}

pub fn codes_schema() -> Schema {
    Schema::new(
        vec![
            Field::new("mcc", DataType::UInt32, true),
            Field::new("mnc", DataType::UInt32, true),
            Field::new("lac", DataType::UInt32, true),
            Field::new("cid", DataType::UInt32, true)
        ]
    )
}

pub fn other_common_measurement_params_schema() -> Schema {
    Schema::new(
        vec![
            Field::new("power", DataType::Float64, true),
            Field::new("delay", DataType::Float64, true)
        ]
    )
}

pub fn gsm_measurement_schema() -> Result<Schema, anyhow::Error> {
    let ps = points_schema();
    let cs = codes_schema();
    let common_params = other_common_measurement_params_schema();
    Schema::try_merge(
        vec![
            ps,
            cs,
            Schema::new(
                vec![
                    // GSMMeasurement
                    Field::new("bsic", DataType::UInt32, true),
                    Field::new("arfcn", DataType::UInt32, true)
                ]
            ),
            common_params
        ]
    ).map_err(|err| anyhow::anyhow!(err))
}

pub fn wcdma_measurement_schema() -> Result<Schema, anyhow::Error> {
    let ps = points_schema();
    let cs = codes_schema();
    let common_params = other_common_measurement_params_schema();
    Schema::try_merge(
        vec![
            ps,
            cs,
            Schema::new(
                vec![
                    // WCDMAMeasurement
                    Field::new("psc", DataType::UInt32, true),
                    Field::new("uarfcn", DataType::UInt32, true)
                ]
            ),
            common_params
        ]
    ).map_err(|err| anyhow::anyhow!(err))
}

pub fn lte_measurement_schema() -> Result<Schema, anyhow::Error> {
    let ps = points_schema();
    let cs = codes_schema();
    let common_params = other_common_measurement_params_schema();
    Schema::try_merge(
        vec![
            ps,
            cs,
            Schema::new(
                vec![
                    // LTEMeasurement
                    Field::new("pci", DataType::UInt32, true),
                    Field::new("earfcn", DataType::UInt32, true)
                ]
            ),
            common_params
        ]
    ).map_err(|err| anyhow::anyhow!(err))
}

// 5GNR
pub fn five_gnr_measurement_schema() -> Result<Schema, anyhow::Error> {
    let ps = points_schema();
    let cs = codes_schema();
    let common_params = other_common_measurement_params_schema();
    Schema::try_merge(
        vec![
            ps,
            cs,
            Schema::new(
                vec![
                    // FiveGNRMeasurement
                    Field::new("pci", DataType::UInt32, true),
                    Field::new("earfcn", DataType::UInt32, true)
                ]
            ),
            common_params
        ]
    ).map_err(|err| anyhow::anyhow!(err))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standart_serialize() {
        let five_gnr = Standart::FiveGNR;
        assert_eq!(five_gnr.as_ref(), "5GNR");
    }
}
