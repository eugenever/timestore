use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Schema;
use lancedb::connect;
use lancedb::table::Table;

use crate::storage::schema::{ get_measurements_metadata_schema, get_points_metadata_schema };

pub async fn init_metadata(path: &str) -> Result<HashMap<String, Table>, anyhow::Error> {
    let conn = connect(&path).execute().await?;
    let tables = conn.table_names().execute().await?;
    let meta_tables_init = metadata_tables_for_init()?;
    let mut metadata_tables: HashMap<String, Table> = HashMap::new();
    for (table_name, schema) in &meta_tables_init {
        let table: Table;
        if !tables.contains(&table_name) {
            table = conn.create_empty_table(table_name, schema.clone()).execute().await?;
        } else {
            table = conn.open_table(table_name).execute().await?;
        }
        metadata_tables.insert(table_name.clone(), table);
    }

    Ok(metadata_tables)
}

pub fn metadata_tables_for_init() -> Result<HashMap<String, Arc<Schema>>, anyhow::Error> {
    let mut tables: HashMap<String, Arc<Schema>> = HashMap::new();
    tables.insert(String::from("points"), Arc::new(get_points_metadata_schema()?));
    tables.insert(String::from("measurements"), Arc::new(get_measurements_metadata_schema()?));
    Ok(tables)
}
