use threadpool::ThreadPool;
use tracing::{ event, Level };

use crate::config::Config;
use crate::storage::models::{ Location, Measurement };
use multilateration::{
    ConfigLMA,
    multilaterate_optimized,
    test::{ lon_lat_to_mercator, mercator_to_lon_lat, mock_measurement_data },
};

#[derive(Debug, Clone)]
pub struct EstimatedLocation {
    // index of measurement in vector (synchronization of results from streams)
    pub index: usize,
    pub longitude: f64,
    pub latitude: f64,
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

pub async fn estimate_location(
    config: &Config,
    pool: &ThreadPool,
    mut measurements: Vec<Measurement>
) -> Vec<Measurement> {
    let (tx, rx) = flume::unbounded::<EstimatedLocation>();
    // enumerate is performed to synchronize measurements from the input vector with the calculation results from the pool
    let temp_measurements: Vec<(usize, &Measurement)> = measurements.iter().enumerate().collect();

    let config_lma = ConfigLMA {
        measurements: vec![],
        decimals: config.mlat.decimals,
        number_iterations: if config.mlat.light_optimization {
            config.mlat.number_iterations_light
        } else {
            config.mlat.number_iterations
        },
        distance_deviation_threshold: config.mlat.distance_deviation_threshold,
        optimize_number_measurements_up_to: config.mlat.optimize_number_measurements_up_to as usize,
        max_height: config.mlat.max_height,
        light_optimization: config.mlat.light_optimization,
    };

    for chunk in temp_measurements.chunks(config.mlat.number_threads as usize) {
        for (index, m) in chunk {
            // TODO: check standards and request stations for them from the Station storage
            if let Some(ref _gsm) = m.gsm {
            }
            if let Some(ref _wcdma) = m.wcdma {
            }
            if let Some(ref _lte) = m.lte {
            }
            if let Some(ref _five_gnr) = m.five_gnr {
            }

            // TODO: only for test
            let mlat_measurements = mock_measurement_data();

            let mut config_lma = config_lma.clone();
            let tx = tx.clone();
            let i = *index;

            pool.execute(move || {
                config_lma.measurements = mlat_measurements;
                let mlat_result = multilaterate_optimized(None, &config_lma);
                match mlat_result {
                    Err(err) => {
                        event!(Level::ERROR, "Error estimate of coordinates: {err}");
                    }
                    Ok(point) => {
                        let x = point.0[0];
                        let y = point.0[1];
                        let z = point.0[2];
                        // TODO: only for test
                        let (longitude, latitude) = mercator_to_lon_lat(x, y);
                        let estimate_loc = EstimatedLocation {
                            index: i,
                            longitude,
                            latitude,
                            x,
                            y,
                            z,
                        };
                        if let Err(err) = tx.send(estimate_loc) {
                            event!(Level::ERROR, "Error send estimate location: {err}");
                        }
                    }
                }
            });
        }
    }

    // needed to complete the loop below which will give an Err variant
    drop(tx);

    let count_measurements = measurements.len();
    while let Ok(estimate_loc) = rx.recv_async().await {
        if estimate_loc.index < count_measurements {
            // synchronization by index from input vector
            measurements[estimate_loc.index].location = Some(Location {
                longitude: estimate_loc.longitude,
                latitude: estimate_loc.latitude,
                x: Some(estimate_loc.x),
                y: Some(estimate_loc.y),
                height_agl: estimate_loc.z,
                height_asl: None,
            });
        }
    }

    measurements
}
