#![allow(unused)]

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u64,
}

#[derive(Debug, Clone)]
pub struct NatsConfig {
    pub enabled: bool,
    pub url: String,
    pub subject: String,
    pub queue: String,
    pub subscription_capacity: usize,
}

#[derive(Debug, Clone)]
pub struct LanceDBConfig {
    pub timeout_pull: u64,
    pub batch_size: u64,
    pub store_path: String,
    pub table_partition_prefix: String,
    pub datasets: Vec<String>,
    pub standarts: Option<Vec<String>>,
    pub max_rows_per_file: u64,
    pub max_rows_per_file_measurement_bs: u64,
    pub optimize_table: bool,
}

#[derive(Debug, Clone)]
pub struct MLATConfig {
    pub number_threads: u64,
    pub decimals: u64,
    pub distance_deviation_threshold: f64,
    pub max_height: f64,
    pub number_iterations: u64,
    pub number_iterations_light: u64,
    pub optimize_number_measurements_up_to: u64,
    pub light_optimization: bool,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub nats: NatsConfig,
    pub lancedb: LanceDBConfig,
    pub log_level: String,
    pub server: ServerConfig,
    pub mlat: MLATConfig,
}

pub async fn load_config(cwd: &str) -> Result<Config, anyhow::Error> {
    let data = tokio::fs::read_to_string("config.json").await?;
    let config_json: serde_json::Value = serde_json
        ::from_str(&data)
        .expect("config.json was not well-formatted");

    // NATS config params
    let nats_enabled = config_json
        .get("nats")
        .and_then(|nats| nats.get("enabled"))
        .and_then(|enabled| enabled.as_bool())
        .unwrap_or(true);

    let nats_url = config_json
        .get("nats")
        .and_then(|nats| nats.get("url"))
        .and_then(|url| url.as_str())
        .expect("Nats url is undefined")
        .to_string();

    let nats_subject = config_json
        .get("nats")
        .and_then(|nats| nats.get("subject"))
        .and_then(|subject| subject.as_str())
        .expect("Nats subject is undefined")
        .to_string();

    let nats_queue = config_json
        .get("nats")
        .and_then(|nats| nats.get("queue"))
        .and_then(|q| q.as_str())
        .unwrap_or("queue.measurements")
        .to_string();

    let nats_subscription_capacity = config_json
        .get("nats")
        .and_then(|nats| nats.get("subscription_capacity"))
        .and_then(|sc| sc.as_u64())
        .unwrap_or(10 * 64 * 1024) as usize;

    let nats_config = NatsConfig {
        enabled: nats_enabled,
        url: nats_url,
        subject: nats_subject,
        queue: nats_queue,
        subscription_capacity: nats_subscription_capacity,
    };

    // LanceDB config params
    let lancedb_timeout_pull = config_json
        .get("lancedb")
        .and_then(|db| db.get("timeout_pull"))
        .and_then(|timeout| timeout.as_u64())
        .unwrap_or(10);

    let lancedb_batch_size = config_json
        .get("lancedb")
        .and_then(|db| db.get("batch_size"))
        .and_then(|batch| batch.as_u64())
        .unwrap_or(1_000);

    let lancedb_store_path = config_json
        .get("lancedb")
        .and_then(|db| db.get("store_path"))
        .and_then(|path| path.as_str())
        .unwrap_or("./")
        .to_string();

    let lancedb_db_table_partition_prefix = config_json
        .get("lancedb")
        .and_then(|db| db.get("table_partition_prefix"))
        .and_then(|table_partition_prefix| table_partition_prefix.as_str())
        .unwrap_or("partition")
        .to_string();

    let lancedb_datasets = config_json
        .get("lancedb")
        .and_then(|db| db.get("datasets"))
        .and_then(|ds| {
            ds.as_array().map(|arr| {
                arr.iter()
                    .map(|ds| ds.as_str().unwrap().to_string())
                    .collect::<Vec<_>>()
            })
        })
        .expect("Datasets of LanceDB is undefined");

    let lancedb_standarts = config_json
        .get("lancedb")
        .and_then(|db| db.get("standarts"))
        .and_then(|standarts| {
            standarts.as_array().map(|arr| {
                arr.iter()
                    .map(|ds| ds.as_str().unwrap().to_string())
                    .collect::<Vec<_>>()
            })
        });

    let lancedb_max_rows_per_file = config_json
        .get("lancedb")
        .and_then(|db| db.get("max_rows_per_file"))
        .and_then(|max_rows| max_rows.as_u64())
        .unwrap_or(1_500_0000);

    let lancedb_max_rows_per_file_measurement_bs = config_json
        .get("lancedb")
        .and_then(|db| db.get("max_rows_per_file_measurement_bs"))
        .and_then(|max_rows| max_rows.as_u64())
        .unwrap_or(300_0000);

    let lancedb_optimize_table = config_json
        .get("lancedb")
        .and_then(|lancedb| lancedb.get("optimize_table"))
        .and_then(|optimize_table| optimize_table.as_bool())
        .unwrap_or(true);

    let lancedb_config = LanceDBConfig {
        batch_size: lancedb_batch_size,
        timeout_pull: lancedb_timeout_pull,
        store_path: lancedb_store_path,
        table_partition_prefix: lancedb_db_table_partition_prefix,
        datasets: lancedb_datasets,
        standarts: lancedb_standarts,
        max_rows_per_file: lancedb_max_rows_per_file,
        max_rows_per_file_measurement_bs: lancedb_max_rows_per_file_measurement_bs,
        optimize_table: lancedb_optimize_table,
    };

    let log_level = config_json
        .get("log_level")
        .and_then(|level| level.as_str())
        .unwrap_or("ERROR")
        .to_string();

    // HTTP Server config params
    let server_host = config_json
        .get("server")
        .and_then(|server| server.get("host"))
        .and_then(|h| h.as_str())
        .unwrap_or("127.0.0.1")
        .to_string();

    let server_port = config_json
        .get("server")
        .and_then(|server| server.get("port"))
        .and_then(|p| p.as_u64())
        .unwrap_or(8000);

    let server_config = ServerConfig {
        host: server_host,
        port: server_port,
    };

    // MLAT config params
    let mlat_number_threads = config_json
        .get("mlat")
        .and_then(|mlat| mlat.get("number_threads"))
        .and_then(|number_threads| number_threads.as_u64())
        .unwrap_or(4);

    let mlat_decimals = config_json
        .get("mlat")
        .and_then(|mlat| mlat.get("decimals"))
        .and_then(|decimals| decimals.as_u64())
        .unwrap_or(20);

    let mlat_distance_deviation_threshold = config_json
        .get("mlat")
        .and_then(|mlat| mlat.get("distance_deviation_threshold"))
        .and_then(|distance_deviation_threshold| distance_deviation_threshold.as_f64())
        .unwrap_or(0.1);

    let mlat_max_height = config_json
        .get("mlat")
        .and_then(|mlat| mlat.get("max_height"))
        .and_then(|max_height| max_height.as_f64())
        .unwrap_or(50.0);

    let mlat_number_iterations = config_json
        .get("mlat")
        .and_then(|mlat| mlat.get("number_iterations"))
        .and_then(|number_iterations| number_iterations.as_u64())
        .unwrap_or(70);

    let mlat_number_iterations_light = config_json
        .get("mlat")
        .and_then(|mlat| mlat.get("number_iterations_light"))
        .and_then(|number_iterations_light| number_iterations_light.as_u64())
        .unwrap_or(30);

    let mlat_optimize_number_measurements_up_to = config_json
        .get("mlat")
        .and_then(|mlat| mlat.get("optimize_number_measurements_up_to"))
        .and_then(|optimize_number_measurements_up_to| {
            optimize_number_measurements_up_to.as_u64()
        })
        .unwrap_or(10);

    let mlat_light_optimization = config_json
        .get("mlat")
        .and_then(|mlat| mlat.get("light_optimization"))
        .and_then(|light_optimization| light_optimization.as_bool())
        .unwrap_or(true);

    let mlat_config = MLATConfig {
        number_threads: mlat_number_threads,
        decimals: mlat_decimals,
        distance_deviation_threshold: mlat_distance_deviation_threshold,
        max_height: mlat_max_height,
        number_iterations: mlat_number_iterations,
        number_iterations_light: mlat_number_iterations_light,
        optimize_number_measurements_up_to: mlat_optimize_number_measurements_up_to,
        light_optimization: mlat_light_optimization,
    };

    Ok(Config {
        nats: nats_config,
        lancedb: lancedb_config,
        log_level,
        server: server_config,
        mlat: mlat_config,
    })
}
