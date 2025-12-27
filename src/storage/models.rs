use std::collections::{HashMap, HashSet};

use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Location {
    // longitude, deg
    pub longitude: f64,
    // latitude, deg
    pub latitude: f64,
    // X = Longitude
    pub x: Option<f64>,
    // Y = Latitude
    pub y: Option<f64>,
    // height Above Ground Level, m
    pub height_agl: f64,
    // height Above Sea Level, m
    pub height_asl: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct GSMMeasurement {
    // Mobile Country Code
    pub mcc: u32,
    // Mobile Network Code
    pub mnc: u32,
    // Location Area Code
    pub lac: u32,
    // Cell ID
    pub cid: u32,
    // Base Station Identity Code
    pub bsic: Option<u32>,
    // Absolute RF Channel Number
    pub arfcn: Option<u32>,
    // Power Received, dBm
    pub power: Option<f64>,
    // Propagation delay, µs
    pub delay: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct WCDMAMeasurement {
    // Mobile Country Code
    pub mcc: u32,
    // Mobile Network Code
    pub mnc: u32,
    // Location Area Code
    pub lac: u32,
    // Cell ID
    pub cid: u32,
    // UMTS Primary Scrambling Code
    pub psc: Option<u32>,
    // UMTS Absolute RF Channel Number
    pub uarfcn: Option<u32>,
    // Power Received, dBm
    pub power: Option<f64>,
    // Propagation delay, µs
    pub delay: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct LTEMeasurement {
    // Mobile Country Code
    pub mcc: u32,
    // Mobile Network Code
    pub mnc: u32,
    // Location Area Code
    pub lac: u32,
    // Cell ID
    pub cid: u32,
    // Physical Cell ID
    pub pci: Option<u32>,
    // UMTS Absolute RF Channel Number
    pub earfcn: Option<u32>,
    // Power Received, dBm
    pub power: Option<f64>,
    // Propagation delay, µs
    pub delay: Option<f64>,
}

// 5GNR
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct FiveGNRMeasurement {
    // Mobile Country Code
    pub mcc: u32,
    // Mobile Network Code
    pub mnc: u32,
    // Location Area Code
    pub lac: u32,
    // Cell ID
    pub cid: u32,
    // Physical Cell ID
    pub pci: Option<u32>,
    // UMTS Absolute RF Channel Number
    pub earfcn: Option<u32>,
    // Power Received, dBm
    pub power: Option<f64>,
    // Propagation delay, µs
    pub delay: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct WiFiMeasurement {
    // MAC address
    pub bssid: String,
    // Extended Service Set Identifier
    pub essid: Option<String>,
    // RF Channel Number
    pub channel: u32,
    // Power Received, dBm
    pub power: Option<f64>,
    // Propagation delay, µs
    pub delay: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ValuePrecision {
    // measured value in specific unit
    pub val: f64,
    // measurement precision in the same unit as value
    pub prc: f64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct UWBMeasurement {}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct BLEMeasurement {}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RFIDMeasurement {}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Measurement {
    // id of measurement
    pub id: Option<u32>,
    // unique device identity
    pub unit_id: String,
    // measurement timestamp
    pub timestamp: chrono::DateTime<Utc>,
    // measured location
    pub location: Option<Location>,
    pub gsm: Option<Vec<GSMMeasurement>>,
    pub wcdma: Option<Vec<WCDMAMeasurement>>,
    pub lte: Option<Vec<LTEMeasurement>>,
    pub five_gnr: Option<Vec<FiveGNRMeasurement>>,
    pub wifi: Option<Vec<WiFiMeasurement>>,
    pub uwb: Option<Vec<UWBMeasurement>>,
    pub ble: Option<Vec<BLEMeasurement>>,
    pub rfid: Option<Vec<RFIDMeasurement>>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct BSCodes {
    pub mcc: u32,
    pub mnc: u32,
    pub lac: u32,
    pub cid: u32,
}

#[derive(Debug)]
pub struct MeasurementsByStandarts<'a> {
    // list of measurements by standards
    pub _measurements_by_standarts: HashMap<String, Vec<&'a Measurement>>,
    // list of base stations by standards
    pub bs_codes_by_standarts: HashMap<String, HashSet<BSCodes>>,
    pub measurements_by_standarts_and_bs_codes:
        HashMap<String, HashMap<BSCodes, Vec<&'a Measurement>>>,
}
