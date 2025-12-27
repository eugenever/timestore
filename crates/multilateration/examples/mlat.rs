use std::collections::HashMap;

use anyhow::Result;
use calamine::{
    deserialize_as_f64_or_none, open_workbook, HeaderRow, RangeDeserializerBuilder,
    Reader, Xlsx,
};

use multilateration::{
    multilaterate_by_method, multilaterate_optimized, ConfigLMA,
    InitialPointSelectionMethod, MLATMeasurement, Point,
};
use rust_xlsxwriter::Workbook;
use serde::Deserialize;

#[derive(Deserialize)]
struct RecordAS {
    #[serde(alias = "Callpign")]
    p: String,

    // Target coordinates
    #[serde(alias = "Xi", deserialize_with = "deserialize_as_f64_or_none")]
    xi: Option<f64>,
    #[serde(alias = "Yi", deserialize_with = "deserialize_as_f64_or_none")]
    yi: Option<f64>,
    #[serde(alias = "H", deserialize_with = "deserialize_as_f64_or_none")]
    h: Option<f64>,

    // Distances
    #[serde(alias = "Distance m1", deserialize_with = "deserialize_as_f64_or_none")]
    distance1: Option<f64>,
    #[serde(alias = "Distance m2", deserialize_with = "deserialize_as_f64_or_none")]
    distance2: Option<f64>,
    #[serde(alias = "Distance m3", deserialize_with = "deserialize_as_f64_or_none")]
    distance3: Option<f64>,
    #[serde(alias = "Distance m4", deserialize_with = "deserialize_as_f64_or_none")]
    distance4: Option<f64>,
    #[serde(alias = "Distance m5", deserialize_with = "deserialize_as_f64_or_none")]
    distance5: Option<f64>,
    #[serde(alias = "Distance m6", deserialize_with = "deserialize_as_f64_or_none")]
    distance6: Option<f64>,

    #[serde(alias = "Distance m7", deserialize_with = "deserialize_as_f64_or_none")]
    distance7: Option<f64>,
    #[serde(alias = "Distance m8", deserialize_with = "deserialize_as_f64_or_none")]
    distance8: Option<f64>,
    #[serde(alias = "Distance m9", deserialize_with = "deserialize_as_f64_or_none")]
    distance9: Option<f64>,
    #[serde(
        alias = "Distance m10",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance10: Option<f64>,
    #[serde(
        alias = "Distance m11",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance11: Option<f64>,
    #[serde(
        alias = "Distance m12",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance12: Option<f64>,

    #[serde(
        alias = "Distance m13",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance13: Option<f64>,
    #[serde(
        alias = "Distance m14",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance14: Option<f64>,
    #[serde(
        alias = "Distance m15",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance15: Option<f64>,
    #[serde(
        alias = "Distance m16",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance16: Option<f64>,
    #[serde(
        alias = "Distance m17",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance17: Option<f64>,
    #[serde(
        alias = "Distance m18",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance18: Option<f64>,

    #[serde(
        alias = "Distance m19",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance19: Option<f64>,
    #[serde(
        alias = "Distance m20",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance20: Option<f64>,
    #[serde(
        alias = "Distance m21",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance21: Option<f64>,
    #[serde(
        alias = "Distance m22",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance22: Option<f64>,
    #[serde(
        alias = "Distance m23",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance23: Option<f64>,
    #[serde(
        alias = "Distance m24",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance24: Option<f64>,

    #[serde(
        alias = "Distance m25",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance25: Option<f64>,
    #[serde(
        alias = "Distance m26",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance26: Option<f64>,
    #[serde(
        alias = "Distance m27",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance27: Option<f64>,
    #[serde(
        alias = "Distance m28",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance28: Option<f64>,
    #[serde(
        alias = "Distance m29",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance29: Option<f64>,
    #[serde(
        alias = "Distance m30",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance30: Option<f64>,
}

#[derive(Deserialize)]
struct Record {
    #[serde(alias = "Xi", deserialize_with = "deserialize_as_f64_or_none")]
    xi: Option<f64>,
    #[serde(alias = "Yi", deserialize_with = "deserialize_as_f64_or_none")]
    yi: Option<f64>,
    #[serde(alias = "H", deserialize_with = "deserialize_as_f64_or_none")]
    _h: Option<f64>,
    #[serde(
        alias = "Distance m28",
        deserialize_with = "deserialize_as_f64_or_none"
    )]
    distance: Option<f64>,
}

// cargo run --package multilateration --example mlat --release
fn main() -> Result<()> {
    // estimate_location_bs_by_as()?;
    // estimate_location_as_by_bs()?;
    estimate_location_as_by_bs_excel()?;
    Ok(())
}

#[allow(unused)]
fn estimate_location_bs_by_as() -> Result<(), anyhow::Error> {
    let path = format!("{}/examples/MLAT_advanced.xlsx", env!("CARGO_MANIFEST_DIR"));
    let mut excel: Xlsx<_> = open_workbook(path)?;
    let range = excel
        .with_header_row(HeaderRow::Row(1))
        .worksheet_range("Points")
        .map_err(|_| calamine::Error::Msg("Cannot find Points"))?;

    let iter_records =
        RangeDeserializerBuilder::with_headers(&["Xi", "Yi", "H", "Distance m28"])
            .from_range(&range)?;

    let mut measurements = vec![];
    let decimals = 2;

    for result in iter_records {
        let record: Record = result?;
        measurements.push(MLATMeasurement::new(
            None,
            Point(vec![
                round_float(record.xi.unwrap(), decimals),
                round_float(record.yi.unwrap(), decimals),
                round_float(record._h.unwrap(), decimals),
                // 0.0,
            ]),
            record.distance.unwrap(),
        ));
    }

    let n = 50;
    let measurements: Vec<MLATMeasurement> = measurements.into_iter().take(n).collect();

    let st = std::time::Instant::now();
    let c = measurements.len();
    let number_iterations = 150;
    let coordinates = multilaterate_by_method(
        measurements,
        InitialPointSelectionMethod::Middle,
        number_iterations,
    )
    .unwrap()
    .0;
    println!("Count measurements: {}, duration: {:?}", c, st.elapsed());
    println!("Coordinates are: {:?}", coordinates);
    Ok(())
}

#[allow(unused)]
fn estimate_location_as_by_bs() -> Result<(), anyhow::Error> {
    let base_stations = vec![
        Point(vec![430715.6562, 6175770.5, 0.0]),
        Point(vec![431135.3438, 6175714.0, 0.0]),
        Point(vec![431613.5, 6175805.5, 0.0]),
        Point(vec![430640.0, 6175701.5, 0.0]),
        Point(vec![431014.0, 6175429.0, 0.0]),
        Point(vec![431030.0, 6175974.0, 0.0]),
    ];
    let distances = vec![343.0, 328.0, 670.0, 447.0, 572.0, 65.0];
    let mut measurements = vec![];

    for (i, p) in base_stations.into_iter().enumerate() {
        measurements.push(MLATMeasurement::new(None, p, distances[i]));
    }

    let st = std::time::Instant::now();
    let c = measurements.len();
    let number_iterations = 150;
    let coordinates = multilaterate_by_method(
        measurements,
        InitialPointSelectionMethod::Middle,
        number_iterations,
    )
    .unwrap()
    .0;
    println!("Count measurements: {}, duration: {:?}", c, st.elapsed());
    println!("Coordinates are: {:?}", coordinates);

    Ok(())
}

#[allow(unused)]
fn estimate_location_as_by_bs_excel() -> Result<(), anyhow::Error> {
    let idx = vec![3, 24, 12, 4];
    let base_stations = bs_with_h(&idx);

    let path = format!("{}/examples/MLAT_advanced.xlsx", env!("CARGO_MANIFEST_DIR"));
    let mut excel: Xlsx<_> = open_workbook(path)?;
    let range = excel
        .with_header_row(HeaderRow::Row(1))
        .worksheet_range("Points")
        .map_err(|_| calamine::Error::Msg("Cannot find Points"))?;

    let iter_records = RangeDeserializerBuilder::with_headers(&[
        "Callpign",
        "Xi",
        "Yi",
        "H",
        "Distance m1",
        "Distance m2",
        "Distance m3",
        "Distance m4",
        "Distance m5",
        "Distance m6",
        "Distance m7",
        "Distance m8",
        "Distance m9",
        "Distance m10",
        "Distance m11",
        "Distance m12",
        "Distance m13",
        "Distance m14",
        "Distance m15",
        "Distance m16",
        "Distance m17",
        "Distance m18",
        "Distance m19",
        "Distance m20",
        "Distance m21",
        "Distance m22",
        "Distance m23",
        "Distance m24",
        "Distance m25",
        "Distance m26",
        "Distance m27",
        "Distance m28",
        "Distance m29",
        "Distance m30",
    ])
    .from_range(&range)?;

    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();

    worksheet.set_column_width(1, 15)?;
    worksheet.set_column_width(2, 15)?;
    worksheet.set_column_width(3, 22)?;
    worksheet.set_column_width(4, 22)?;
    worksheet.set_column_width(5, 22)?;
    worksheet.set_column_width(6, 22)?;
    worksheet.set_column_width(7, 22)?;

    let mut row = 0;
    // Create headers
    worksheet.write(row, 0, "Callpign")?;
    worksheet.write(row, 1, "X_origin")?;
    worksheet.write(row, 2, "Y_origin")?;
    worksheet.write(row, 3, "X_estimate")?;
    worksheet.write(row, 4, "Y_estimate")?;
    worksheet.write(row, 5, "H_estimate")?;
    worksheet.write(row, 6, "Error XY, meters")?;
    worksheet.write(row, 7, "Error XYZ, meters")?;
    row += 1;

    let decimals = 20;
    let mut ie = 1u16;
    let mut max_error_xy = 0.0;
    for result in iter_records {
        let mut measurements = vec![];
        let record: RecordAS = result?;
        for (i, p) in &base_stations {
            let distance = match *i {
                1 => record.distance1,
                2 => record.distance2,
                3 => record.distance3,
                4 => record.distance4,
                5 => record.distance5,
                6 => record.distance6,
                7 => record.distance7,
                8 => record.distance8,
                9 => record.distance9,
                10 => record.distance10,
                11 => record.distance11,
                12 => record.distance12,
                13 => record.distance13,
                14 => record.distance14,
                15 => record.distance15,
                16 => record.distance16,
                17 => record.distance17,
                18 => record.distance18,
                19 => record.distance19,
                20 => record.distance20,
                21 => record.distance21,
                22 => record.distance22,
                23 => record.distance23,
                24 => record.distance24,
                25 => record.distance25,
                26 => record.distance26,
                27 => record.distance27,
                28 => record.distance28,
                29 => record.distance29,
                30 => record.distance30,
                _ => None,
            };
            measurements.push(MLATMeasurement::new(
                None,
                p.clone(),
                round_float(distance.unwrap(), decimals),
            ));
        }

        let c = measurements.len();
        let st = std::time::Instant::now();

        let light_optimization = true;
        let number_iterations = if light_optimization { 30 } else { 70 };
        let config = ConfigLMA {
            measurements,
            decimals: 20,
            distance_deviation_threshold: 0.1,
            max_height: 50.0,
            number_iterations,
            optimize_number_measurements_up_to: 10,
            light_optimization,
        };
        let estimate_coordinates =
            multilaterate_optimized(Some(record.p.clone()), &config)
                .map_err(|err| {
                    println!("ERROR: {:?}", err);
                    err
                })
                .unwrap()
                .0;

        println!("Count measurements: {}, duration: {:?}", c, st.elapsed());

        let error_xy = ((record.xi.unwrap() - estimate_coordinates[0]).powf(2.0)
            + (record.yi.unwrap() - estimate_coordinates[1]).powf(2.0))
        .sqrt();
        let error_xyz = ((record.xi.unwrap() - estimate_coordinates[0]).powf(2.0)
            + (record.yi.unwrap() - estimate_coordinates[1]).powf(2.0)
            + (record.h.unwrap() - estimate_coordinates[2]).powf(2.0))
        .sqrt();

        if error_xy > max_error_xy {
            max_error_xy = error_xy
        }
        // TODO:
        if error_xyz >= 2.0 {
            println!(
                "{}, {}, error_xy: {:.3}, error_xyz: {:.3}, target: [{}, {}, {}], estimate coordinates: {:?}",
                ie,
                record.p,
                error_xy,
                error_xyz,
                record.xi.unwrap(),
                record.yi.unwrap(),
                record.h.unwrap(),
                estimate_coordinates
            );
            ie += 1;
        }

        worksheet.write(row, 0, record.p)?;
        worksheet.write(row, 1, record.xi.unwrap())?;
        worksheet.write(row, 2, record.yi.unwrap())?;
        worksheet.write(row, 3, estimate_coordinates[0])?;
        worksheet.write(row, 4, estimate_coordinates[1])?;
        worksheet.write(row, 5, estimate_coordinates[2])?;
        worksheet.write(row, 6, error_xy)?;
        worksheet.write(row, 7, error_xyz)?;
        row += 1;
    }

    workbook.save(format!(
        "{}/examples/estimates/estimate_location_as_by_bs_{}_optimized.xlsx",
        env!("CARGO_MANIFEST_DIR"),
        base_stations.len(),
    ))?;

    println!("\nMAX XY Error: {max_error_xy}");

    Ok(())
}

// Z = Hant
#[allow(unused)]
fn bs_with_h(idx: &[u16]) -> HashMap<u16, Point> {
    let mut base_stations = HashMap::new();

    base_stations.insert(1u16, Point(vec![430715.6562, 6175770.5, 15.0]));
    base_stations.insert(2, Point(vec![431135.3438, 6175714.0, 30.0]));
    base_stations.insert(3, Point(vec![431613.5, 6175805.5, 20.0]));
    base_stations.insert(4, Point(vec![430640.0, 6175701.5, 25.0]));
    base_stations.insert(5, Point(vec![431014.0, 6175429.0, 40.0]));
    base_stations.insert(6, Point(vec![431030.0, 6175974.0, 25.0]));

    base_stations.insert(7, Point(vec![430773.0, 6175921.0, 25.0]));
    base_stations.insert(8, Point(vec![430696.0, 6176028.0, 25.0]));
    base_stations.insert(9, Point(vec![430528.0, 6175930.0, 30.0]));
    base_stations.insert(10, Point(vec![430379.0, 6175970.0, 25.0]));
    base_stations.insert(11, Point(vec![430385.0, 6175658.0, 10.0]));
    base_stations.insert(12, Point(vec![430511.0, 6175808.0, 20.0]));

    base_stations.insert(13, Point(vec![430520.0, 6175429.0, 40.0]));
    base_stations.insert(14, Point(vec![430713.0, 6175567.0, 35.0]));
    base_stations.insert(15, Point(vec![430478.0, 6175597.0, 25.0]));
    base_stations.insert(16, Point(vec![430795.0, 6175380.0, 20.0]));
    base_stations.insert(17, Point(vec![431332.0, 6175362.0, 15.0]));
    base_stations.insert(18, Point(vec![431536.0, 6175472.0, 10.0]));

    base_stations.insert(19, Point(vec![431524.0, 6175616.0, 30.0]));
    base_stations.insert(20, Point(vec![431470.0, 6175960.0, 25.0]));
    base_stations.insert(21, Point(vec![431281.0, 6176106.0, 25.0]));
    base_stations.insert(22, Point(vec![431633.0, 6176089.0, 20.0]));
    base_stations.insert(23, Point(vec![431478.0, 6175799.0, 10.0]));
    base_stations.insert(24, Point(vec![431222.0, 6175883.0, 15.0]));

    base_stations.insert(25, Point(vec![430953.0, 6175680.0, 35.0]));
    base_stations.insert(26, Point(vec![431321.0, 6175526.0, 25.0]));
    base_stations.insert(27, Point(vec![431132.0, 6175302.0, 15.0]));
    base_stations.insert(28, Point(vec![430622.0, 6175304.0, 20.0]));
    base_stations.insert(29, Point(vec![430895.0, 6176095.0, 25.0]));
    base_stations.insert(30, Point(vec![430901.0, 6175280.0, 30.0]));

    let mut selected_stations = HashMap::new();
    for (i, p) in &base_stations {
        if idx.contains(i) {
            selected_stations.insert(*i, p.clone());
        }
    }
    selected_stations
}

// Z = 0
#[allow(unused)]
fn bs_without_h(idx: &[u16]) -> HashMap<u16, Point> {
    let mut base_stations = HashMap::new();
    let h = 0.0;

    base_stations.insert(1u16, Point(vec![430715.6562, 6175770.5, h]));
    base_stations.insert(2, Point(vec![431135.3438, 6175714.0, h]));
    base_stations.insert(3, Point(vec![431613.5, 6175805.5, h]));
    base_stations.insert(4, Point(vec![430640.0, 6175701.5, h]));
    base_stations.insert(5, Point(vec![431014.0, 6175429.0, h]));
    base_stations.insert(6, Point(vec![431030.0, 6175974.0, h]));

    base_stations.insert(7, Point(vec![430773.0, 6175921.0, h]));
    base_stations.insert(8, Point(vec![430696.0, 6176028.0, h]));
    base_stations.insert(9, Point(vec![430528.0, 6175930.0, h]));
    base_stations.insert(10, Point(vec![430379.0, 6175970.0, h]));
    base_stations.insert(11, Point(vec![430385.0, 6175658.0, h]));
    base_stations.insert(12, Point(vec![430511.0, 6175808.0, h]));

    base_stations.insert(13, Point(vec![430520.0, 6175429.0, h]));
    base_stations.insert(14, Point(vec![430713.0, 6175567.0, h]));
    base_stations.insert(15, Point(vec![430478.0, 6175597.0, h]));
    base_stations.insert(16, Point(vec![430795.0, 6175380.0, h]));
    base_stations.insert(17, Point(vec![431332.0, 6175362.0, h]));
    base_stations.insert(18, Point(vec![431536.0, 6175472.0, h]));

    base_stations.insert(19, Point(vec![431524.0, 6175616.0, h]));
    base_stations.insert(20, Point(vec![431470.0, 6175960.0, h]));
    base_stations.insert(21, Point(vec![431281.0, 6176106.0, h]));
    base_stations.insert(22, Point(vec![431633.0, 6176089.0, h]));
    base_stations.insert(23, Point(vec![431478.0, 6175799.0, h]));
    base_stations.insert(24, Point(vec![431222.0, 6175883.0, h]));

    base_stations.insert(25, Point(vec![430953.0, 6175680.0, h]));
    base_stations.insert(26, Point(vec![431321.0, 6175526.0, h]));
    base_stations.insert(27, Point(vec![431132.0, 6175302.0, h]));
    base_stations.insert(28, Point(vec![430622.0, 6175304.0, h]));
    base_stations.insert(29, Point(vec![430895.0, 6176095.0, h]));
    base_stations.insert(30, Point(vec![430901.0, 6175280.0, h]));

    let mut selected_stations = HashMap::new();
    for (i, p) in &base_stations {
        if idx.contains(i) {
            selected_stations.insert(*i, p.clone());
        }
    }
    selected_stations
}

fn round_float(x: f64, decimals: u32) -> f64 {
    let y = 10i32.pow(decimals) as f64;
    (x * y).round() / y
}
