#![allow(unused)]
pub mod lance_polars;

pub use lance_polars::lance_table_to_polars_df;

use arrow_array::RecordBatch;
use polars::series::Series;

pub fn arrow_array_to_polars_series(
    batch: &RecordBatch,
    column_name: &str,
    series: &mut Series
) -> Result<(), anyhow::Error> {
    let arrow_array = batch
        .column_by_name(column_name)
        .ok_or(anyhow::anyhow!("Error get column arrow array for '{}'", column_name))?;
    let polars_array: polars_arrow::array::ArrayRef = arrow_array.as_ref().into();
    series.append(&Series::from_arrow(column_name, polars_array)?)?;
    Ok(())
}
