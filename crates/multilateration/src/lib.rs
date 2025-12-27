use std::collections::HashMap;

use anyhow::{anyhow, bail, Result};
use mathru::algebra::linear::matrix::matrix::Matrix;
use mathru::algebra::linear::vector::vector::Vector;
use mathru::elementary::Power;
use mathru::optimization::{LevenbergMarquardt, Optim};
use strum_macros::{AsRefStr, Display};

#[derive(Debug, Clone, Copy, Display, AsRefStr)]
pub enum InitialPointSelectionMethod {
    #[strum(serialize = "nearest")]
    Nearest,
    #[strum(serialize = "middle")]
    Middle,
    #[strum(serialize = "far")]
    Far,
}

struct MultilaterationFunction {
    measurements: Vec<MLATMeasurement>,
}

impl MultilaterationFunction {
    pub fn new(measurements: Vec<MLATMeasurement>) -> MultilaterationFunction {
        MultilaterationFunction { measurements }
    }

    pub fn estimate_intial_point(
        &mut self,
        method: Option<InitialPointSelectionMethod>,
        offset: Option<usize>,
    ) -> Result<Point, anyhow::Error> {
        let position_dimensions = self.measurements[0].point.0.len();
        let number_of_measurements = self.measurements.len();

        let mut initial_position = vec![0f64; position_dimensions];

        if let Some(_offset) = offset {
            self.measurements
                .sort_by(|v0, v1| v0.distance.partial_cmp(&v1.distance).unwrap());

            if _offset > 0 && _offset < number_of_measurements {
                for j in 0..position_dimensions {
                    initial_position[j] = self.measurements[_offset].point.0[j];
                }
            } else {
                // if offset invalid use Nearest point (index = 0)
                for j in 0..position_dimensions {
                    initial_position[j] = self.measurements[0].point.0[j];
                }
            }
        } else {
            if let Some(m) = method {
                match m {
                    InitialPointSelectionMethod::Nearest => {
                        self.measurements.sort_by(|v0, v1| {
                            v0.distance.partial_cmp(&v1.distance).unwrap()
                        });
                        for j in 0..position_dimensions {
                            initial_position[j] = self.measurements[0].point.0[j];
                        }
                    }
                    InitialPointSelectionMethod::Middle => {
                        for i in 0..number_of_measurements {
                            for j in 0..position_dimensions {
                                initial_position[j] += self.measurements[i].point.0[j];
                            }
                        }
                        for i in 0..position_dimensions {
                            initial_position[i] /= number_of_measurements as f64;
                        }
                    }
                    InitialPointSelectionMethod::Far => {
                        self.measurements.sort_by(|v0, v1| {
                            v0.distance.partial_cmp(&v1.distance).unwrap()
                        });
                        for j in 0..position_dimensions {
                            initial_position[j] =
                                self.measurements[number_of_measurements - 1].point.0[j];
                        }
                    }
                }
            } else {
                return Err(anyhow!(
                    "'estimate_intial_point': one of the parameters must be specified 'offset' or 'method'"
                ));
            }
        }

        Ok(Point(initial_position))
    }
}

impl Optim<f64> for MultilaterationFunction {
    fn eval(&self, input: &Vector<f64>) -> Vector<f64> {
        // output
        let mut result = vec![0f64; self.measurements.len()];
        // compute least squares
        for i in 0..self.measurements.len() {
            // calculate sum, add to overall
            for j in 0..input.clone().convert_to_vec().len() {
                result[i] +=
                    f64::powf(*input.get(j) - self.measurements[i].point.0[j], 2f64);
            }
            result[i] -= f64::powf(self.measurements[i].distance, 2f64);
        }
        Vector::new_column(self.measurements.len(), result)
    }

    fn jacobian(&self, input: &Vector<f64>) -> Matrix<f64> {
        let input_length = input.clone().convert_to_vec().len();
        let data = vec![0f64; self.measurements.len() * input_length];
        // output
        let mut matrix = Matrix::new(self.measurements.len(), input_length, data);
        for i in 0..self.measurements.len() {
            for j in 0..input_length {
                *matrix.get_mut(i, j) =
                    2f64 * input.get(j) - 2f64 * self.measurements[i].point.0[j];
            }
        }
        matrix
    }
}

#[derive(Debug, Clone)]
pub struct Point(pub Vec<f64>);

#[derive(Debug, Clone)]
pub struct MLATMeasurement {
    pub id: Option<String>,
    pub point: Point,
    pub distance: f64,
}

impl MLATMeasurement {
    pub fn new(id: Option<String>, point: Point, distance: f64) -> MLATMeasurement {
        MLATMeasurement {
            id,
            point,
            distance,
        }
    }
}

fn validate_measurements(measurements: &[MLATMeasurement]) -> Result<()> {
    let point_dimensions: Vec<usize> = measurements
        .iter()
        .map(|measurement| measurement.point.0.len())
        .collect();
    let min_length = *point_dimensions
        .iter()
        .min()
        .ok_or(anyhow!("Failed to calculate minimum dimension"))?;
    let max_length = *point_dimensions
        .iter()
        .max()
        .ok_or(anyhow!("Failed to calculate maximum dimension"))?;
    if min_length != max_length {
        bail!("All points must have the same dimensions");
    }
    if min_length < 1 {
        bail!("Points must contain at least one dimension");
    }
    Ok(())
}

pub fn multilaterate_by_method(
    measurements: Vec<MLATMeasurement>,
    method: InitialPointSelectionMethod,
    number_iterations: u64,
) -> Result<Point> {
    validate_measurements(&measurements)?;
    let mut multilateration_function = MultilaterationFunction::new(measurements);
    let optimization = LevenbergMarquardt::new(number_iterations, -1f64, 1f64);
    let initial_point =
        multilateration_function.estimate_intial_point(Some(method), None)?;
    let result = optimization
        .minimize(
            &multilateration_function,
            &Vector::new_column(initial_point.0.len(), initial_point.0),
        )
        .map_err(|err| anyhow!("Failed to calculate a result: {:?}", err))?;
    let coordinates = result.arg().convert_to_vec();
    Ok(Point(coordinates))
}

#[derive(Debug, Clone)]
pub struct ConfigLMA {
    pub measurements: Vec<MLATMeasurement>,
    // number of iterations in the LMA method
    pub number_iterations: u64,
    // threshold value of deviation in distance between BS and the desired point
    pub distance_deviation_threshold: f64,
    // limiting the maximum height of the desired point
    pub max_height: f64,
    // number of measurements at which to disable optimization
    pub optimize_number_measurements_up_to: usize,
    pub decimals: u64,
    // lightweight optimization
    pub light_optimization: bool,
}

pub fn multilaterate_optimized(
    _id_point: Option<String>,
    config: &ConfigLMA,
) -> Result<Point> {
    validate_measurements(&config.measurements)?;
    let mut multilateration_function =
        MultilaterationFunction::new(config.measurements.clone());
    let optimization = LevenbergMarquardt::new(config.number_iterations, -1f64, 1f64);

    // initial optimization by the method of Nearest
    let nearest_initial_point = multilateration_function
        .estimate_intial_point(Some(InitialPointSelectionMethod::Nearest), None)?;
    let nearest_distance = multilateration_function.measurements[0].distance;
    let result = optimization
        .minimize(
            &multilateration_function,
            &Vector::new_column(
                nearest_initial_point.0.len(),
                nearest_initial_point.0.clone(),
            ),
        )
        .map_err(|err| {
            anyhow!(
                "Failed to calculate a result for 'Nearest' method: {:?}",
                err
            )
        })?;

    // specify the obtained distances
    let m = config.measurements.len();
    let mut coordinates = result.arg().convert_to_vec();
    let mut deviations_distance = Vec::with_capacity(m);
    let mut points: Vec<Vec<f64>> = Vec::new();
    let mut dd_points: Vec<Vec<f64>> = Vec::new();
    config.measurements.iter().for_each(|m| {
        let dd = ((m.point.0[0] - coordinates[0]).powf(2.0)
            + (m.point.0[1] - coordinates[1]).powf(2.0)
            + (m.point.0[2] - coordinates[2]).powf(2.0))
        .sqrt();
        deviations_distance.push((m.distance - dd).abs());
    });

    // coefficient of deviation of calculated distances from actual ones
    let mut rate_dd = deviations_distance.iter().map(|d| d.abs()).sum::<f64>();

    add_point(
        &mut points,
        &mut dd_points,
        coordinates.clone(),
        deviations_distance.clone(),
        config.decimals,
        config.max_height,
    );

    if m < config.optimize_number_measurements_up_to {
        // adjustment by Middle method
        let middle_initial_point = multilateration_function
            .estimate_intial_point(Some(InitialPointSelectionMethod::Middle), None)?;
        let adjusted_result = optimization
            .minimize(
                &multilateration_function,
                &Vector::new_column(middle_initial_point.0.len(), middle_initial_point.0),
            )
            .map_err(|err| {
                anyhow!(
                    "Failed to calculate a result for 'Middle' method: {:?}",
                    err
                )
            })?;
        let adjusted_coordinates = adjusted_result.arg().convert_to_vec();

        let mut adjusted_deviations_distance = Vec::with_capacity(m);
        config.measurements.iter().for_each(|m| {
            let dd = ((m.point.0[0] - adjusted_coordinates[0]).powf(2.0)
                + (m.point.0[1] - adjusted_coordinates[1]).powf(2.0)
                + (m.point.0[2] - adjusted_coordinates[2]).powf(2.0))
            .sqrt();
            adjusted_deviations_distance.push((m.distance - dd).abs());
        });
        let adjusted_rate_dd = adjusted_deviations_distance
            .iter()
            .map(|d| d.abs())
            .sum::<f64>();

        if round_float(rate_dd, config.decimals)
            > round_float(adjusted_rate_dd, config.decimals)
            || deviations_has_improved(
                &deviations_distance,
                &adjusted_deviations_distance,
                config.decimals,
            )?
        {
            add_point(
                &mut points,
                &mut dd_points,
                adjusted_coordinates.clone(),
                adjusted_deviations_distance.clone(),
                config.decimals,
                config.max_height,
            );
            rate_dd = adjusted_rate_dd;
            coordinates = adjusted_coordinates;
        }

        let chunks = if config.light_optimization {
            2u16
        } else {
            6u16
        };
        let rate_distance = if config.light_optimization { 1.5 } else { 1.2 };
        let step = rate_distance * nearest_distance / chunks as f64;
        let mut index = 1u16;
        let nearest_coordinates = vec![
            nearest_initial_point.0[0],
            nearest_initial_point.0[1],
            nearest_initial_point.0[2],
        ];

        loop {
            if index > chunks {
                break;
            }

            let heights = if config.light_optimization {
                vec![nearest_coordinates[2] / 2.0]
            } else {
                vec![nearest_coordinates[2] / 2.0, nearest_coordinates[2]]
            };
            for h in heights {
                let initial_points = get_initial_points(
                    &nearest_coordinates,
                    index,
                    step,
                    h,
                    config.light_optimization,
                );

                for (_i, pos_initial_point) in initial_points.into_iter().enumerate() {
                    let pos_result = optimization
                        .minimize(
                            &multilateration_function,
                            &Vector::new_column(
                                pos_initial_point.0.len(),
                                pos_initial_point.0.clone(),
                            ),
                        )
                        .map_err(|err| {
                            anyhow!("Failed to calculate a position result: {:?}", err)
                        })?;

                    let pos_coordinates = pos_result.arg().convert_to_vec();
                    let mut pos_deviations_distance = Vec::with_capacity(m);
                    config.measurements.iter().for_each(|m| {
                        let dd = ((m.point.0[0] - pos_coordinates[0]).powf(2.0)
                            + (m.point.0[1] - pos_coordinates[1]).powf(2.0)
                            + (m.point.0[2] - pos_coordinates[2]).powf(2.0))
                        .sqrt();
                        pos_deviations_distance.push((m.distance - dd).abs());
                    });

                    let pos_rate_dd =
                        pos_deviations_distance.iter().map(|d| d.abs()).sum::<f64>();
                    if round_float(rate_dd, config.decimals)
                        > round_float(pos_rate_dd, config.decimals)
                    {}

                    add_point(
                        &mut points,
                        &mut dd_points,
                        pos_coordinates,
                        pos_deviations_distance,
                        config.decimals,
                        config.max_height,
                    );
                }
            }

            index += 1;
        }

        let count_points = points.len();
        if count_points > 0 {
            // key - index of BS, value - (index of point, deviation)
            let mut min_dd: HashMap<usize, (usize, f64)> = HashMap::new();

            // we specify the true deviations of distances taking into account the height
            let mut clarification_deviations_distance: Vec<Vec<f64>> =
                Vec::with_capacity(count_points);
            for p in points.iter() {
                if p[2] < 0.0 {
                    continue;
                }
                let mut dd_point = Vec::with_capacity(m);
                for m in &config.measurements {
                    let dd = ((m.point.0[0] - p[0]).powf(2.0)
                        + (m.point.0[1] - p[1]).powf(2.0)
                        + (m.point.0[2] - p[2]).powf(2.0))
                    .sqrt();
                    dd_point.push((m.distance - dd).abs());
                }
                clarification_deviations_distance.push(dd_point);
            }

            /*
                min_dd: hash stores information about points with minimal deviations in distances from all stations
                idx_bs: index of BS
                idx_point: index of point
                value: clarificated deviation in distance between BS and point
            */
            for (idx_point, dd) in clarification_deviations_distance.iter().enumerate() {
                // Z(H) of estimate point must be >= 0
                if points[idx_point][2] >= 0.0 {
                    for (idx_bs, value) in dd.iter().enumerate() {
                        if let Some((_index_point, dev)) = min_dd.get(&idx_bs) {
                            if dev > value {
                                min_dd.insert(idx_bs, (idx_point, *value));
                            }
                        } else {
                            min_dd.insert(idx_bs, (idx_point, *value));
                        }
                    }
                }
            }

            /*
            point_freq: hash with the frequency of points having the minimum deviation in distances
            */
            let mut point_freq: HashMap<usize, usize> = HashMap::new();
            min_dd
                .iter()
                .for_each(|(_idx_bs, (idx_point, _deviation))| {
                    if let Some(c) = point_freq.get_mut(idx_point) {
                        *c = *c + 1;
                    } else {
                        point_freq.insert(*idx_point, 1);
                    }
                });

            // index_point: the most frequent point (has the smallest deviation of all)
            // there may be several such points in cases where the nonlinear system has two or more roots
            let index_point = point_freq.iter().max_by_key(|entry| entry.1).unwrap().0;
            coordinates = points[*index_point].clone();
        }
    }

    Ok(Point(coordinates))
}

fn get_initial_points(
    nearest_coordinates: &[f64],
    index: u16,
    step: f64,
    h: f64,
    light_optimization: bool,
) -> Vec<Point> {
    if light_optimization {
        // use the nearest_coordinates
        // set a initial point on the radius rate_dd in 8 fixed positions
        // in azimuths 0, 90, 180, 270 degrees
        vec![
            Point(vec![
                nearest_coordinates[0],
                nearest_coordinates[1] + index as f64 * step,
                h,
            ]),
            Point(vec![
                nearest_coordinates[0] + index as f64 * step,
                nearest_coordinates[1],
                h,
            ]),
            Point(vec![
                nearest_coordinates[0],
                nearest_coordinates[1] - index as f64 * step,
                h,
            ]),
            Point(vec![
                nearest_coordinates[0] - index as f64 * step,
                nearest_coordinates[1],
                h,
            ]),
        ]
    } else {
        // use the nearest_coordinates
        // set a initial point on the radius rate_dd in 8 fixed positions
        // in azimuths 0, 45, 90, 135, 180, 225, 270, 315 degrees
        vec![
            Point(vec![
                nearest_coordinates[0],
                nearest_coordinates[1] + index as f64 * step,
                h,
            ]),
            Point(vec![
                nearest_coordinates[0] + index as f64 * step / 2.0.sqrt(),
                nearest_coordinates[1] + index as f64 * step / 2.0.sqrt(),
                h,
            ]),
            Point(vec![
                nearest_coordinates[0] + index as f64 * step,
                nearest_coordinates[1],
                h,
            ]),
            Point(vec![
                nearest_coordinates[0] + index as f64 * step / 2.0.sqrt(),
                nearest_coordinates[1] - index as f64 * step / 2.0.sqrt(),
                h,
            ]),
            Point(vec![
                nearest_coordinates[0],
                nearest_coordinates[1] - index as f64 * step,
                h,
            ]),
            Point(vec![
                nearest_coordinates[0] - index as f64 * step / 2.0.sqrt(),
                nearest_coordinates[1] - index as f64 * step / 2.0.sqrt(),
                h,
            ]),
            Point(vec![
                nearest_coordinates[0] - index as f64 * step,
                nearest_coordinates[1],
                h,
            ]),
            Point(vec![
                nearest_coordinates[0] - index as f64 * step / 2.0.sqrt(),
                nearest_coordinates[1] + index as f64 * step / 2.0.sqrt(),
                h,
            ]),
        ]
    }
}

fn add_point(
    points: &mut Vec<Vec<f64>>,
    dd_points: &mut Vec<Vec<f64>>,
    coordinates: Vec<f64>,
    dd: Vec<f64>,
    decimals: u64,
    max_height: f64,
) {
    if coordinates[2] > max_height || coordinates[2] < 0.0 {
        return;
    }
    let x = round_float(coordinates[0], decimals);
    let y = round_float(coordinates[1], decimals);
    let z = round_float(coordinates[2], decimals);
    if !points.iter().any(|p| {
        p[0].floor() == x.floor()
            && p[1].floor() == y.floor()
            && p[2].floor() == z.floor()
    }) {
        points.push(vec![x, y, z]);
        dd_points.push(dd);
    }
}

fn deviations_has_improved(
    current_deviation: &[f64],
    new_deviation: &[f64],
    decimals: u64,
) -> Result<bool, anyhow::Error> {
    let m = current_deviation.len();
    let half = (m as f64 / 2.0).floor() as u16;
    let matching = current_deviation
        .iter()
        .zip(new_deviation)
        .filter(|&(c, n)| round_float(*c, decimals) > round_float(*n, decimals))
        .count() as u16;
    if matching >= half {
        return Ok(true);
    }
    Ok(false)
}

fn round_float(x: f64, decimals: u64) -> f64 {
    let y = 10i32.pow(decimals as u32) as f64;
    (x * y).round() / y
}

pub mod test {
    use super::{MLATMeasurement, Point};
    use std::f64::consts::PI;

    // TODO: Only test case
    /// Returns the Spherical Mercator (x, y) in meters
    pub fn lon_lat_to_mercator(lon: f64, lat: f64) -> (f64, f64) {
        let x = 6378137.0 * lon.to_radians();
        let y = 6378137.0 * ((PI * 0.25) + (0.5 * lat.to_radians())).tan().ln();
        (x, y)
    }

    // TODO: Only test case
    pub fn mercator_to_lon_lat(x: f64, y: f64) -> (f64, f64) {
        let lon = (x / 20037508.34) * 180.;
        let mut lat = (y / 20037508.34) * 180.;
        lat = 180. / PI * (2. * (lat * PI / 180.).exp().atan() - PI / 2.);
        (lon, lat)
    }

    // TODO: Only test case
    pub fn mock_measurement_data() -> Vec<MLATMeasurement> {
        let m = vec![
            MLATMeasurement {
                id: None,
                point: Point(vec![430715.6562, 6175770.5, 15.0]),
                distance: 372.978738534041,
            },
            MLATMeasurement {
                id: None,
                point: Point(vec![431135.3438, 6175714.0, 30.0]),
                distance: 660.769182240223,
            },
            MLATMeasurement {
                id: None,
                point: Point(vec![431613.5, 6175805.5, 20.0]),
                distance: 1042.02798925941,
            },
            MLATMeasurement {
                id: None,
                point: Point(vec![430640.0, 6175701.5, 25.0]),
                distance: 431.075260714414,
            },
            MLATMeasurement {
                id: None,
                point: Point(vec![431014.0, 6175429.0, 40.0]),
                distance: 804.511050514535,
            },
            MLATMeasurement {
                id: None,
                point: Point(vec![431030.0, 6175974.0, 25.0]),
                distance: 435.935351170331,
            },
        ];
        m
    }
}
