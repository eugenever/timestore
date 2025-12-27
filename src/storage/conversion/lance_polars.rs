use std::collections::HashMap;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use futures::StreamExt;
use lance::Dataset;
use lancedb::table::Table;
use polars::prelude::*;

pub async fn lance_table_to_polars_df(
    t: &Table,
    exclude_fields: Option<Vec<String>>,
    filter: Option<String>,
) -> Result<DataFrame, anyhow::Error> {
    let columns_with_dtype = table_columns_with_polars_dtype(t, exclude_fields).await?;
    let column_names = columns_with_dtype
        .keys()
        .map(|column| column.as_str())
        .collect::<Vec<_>>();

    let uri = t.dataset_uri();
    let ds = Dataset::open(uri).await?;
    let mut scanner = ds.scan();
    scanner.project(&column_names)?;
    if let Some(f) = filter {
        scanner.filter(&f)?;
    }

    let mut batch_stream = scanner.try_into_stream().await?.map(|b| b.unwrap());

    let mut series = Vec::with_capacity(columns_with_dtype.len());
    for (column, pl_type) in &columns_with_dtype {
        let s = Series::new_empty(column, pl_type);
        series.push(s);
    }

    let mut ref_series = series.iter_mut().map(|s| s).collect::<Vec<_>>();
    while let Some(batch) = batch_stream.next().await {
        arrow_array_to_polars_series(&batch, &column_names, &mut ref_series)?;
    }

    DataFrame::new(series).map_err(|err| anyhow::anyhow!("{err}"))
}

pub async fn table_columns_with_polars_dtype(
    t: &Table,
    exclude_fields: Option<Vec<String>>,
) -> Result<HashMap<String, polars::datatypes::DataType>, anyhow::Error> {
    let schema = t.schema().await?;
    let fields = schema.fields();
    let mut columns = HashMap::with_capacity(fields.len());
    for field in fields {
        let name = field.name().clone();
        if let Some(ref exc_fields) = exclude_fields {
            if exc_fields.contains(&name) {
                continue;
            }
        }
        let pl_type: polars::datatypes::DataType =
            arrow_dt_to_polars_dt(field.data_type());
        columns.insert(name, pl_type);
    }
    Ok(columns)
}

pub fn arrow_dt_to_polars_dt(
    arrow_dt: &arrow_schema::DataType,
) -> polars::datatypes::DataType {
    match arrow_dt {
        arrow_schema::DataType::Utf8 => polars::datatypes::DataType::String,
        arrow_schema::DataType::UInt8 => polars::datatypes::DataType::UInt8,
        arrow_schema::DataType::UInt16 => polars::datatypes::DataType::UInt16,
        arrow_schema::DataType::UInt32 => polars::datatypes::DataType::UInt32,
        arrow_schema::DataType::UInt64 => polars::datatypes::DataType::UInt64,
        arrow_schema::DataType::Int8 => polars::datatypes::DataType::Int8,
        arrow_schema::DataType::Int16 => polars::datatypes::DataType::Int16,
        arrow_schema::DataType::Int32 => polars::datatypes::DataType::Int32,
        arrow_schema::DataType::Int64 => polars::datatypes::DataType::Int64,
        arrow_schema::DataType::Float32 => polars::datatypes::DataType::Float32,
        arrow_schema::DataType::Float64 => polars::datatypes::DataType::Float64,
        arrow_schema::DataType::Binary => polars::datatypes::DataType::Binary,
        _ => unimplemented!(),
    }
}

pub fn arrow_array_to_polars_series(
    batch: &RecordBatch,
    column_names: &[&str],
    series: &mut [&mut Series],
) -> Result<(), anyhow::Error> {
    for (i, column_name) in column_names.iter().enumerate() {
        let arrow_array = batch.column_by_name(column_name).ok_or(anyhow::anyhow!(
            "Invalid column name in mpt '{}'",
            column_name
        ))?;
        let pl_array: polars_arrow::array::ArrayRef = arrow_array.as_ref().into();
        series[i].append(&Series::from_arrow(column_name, pl_array)?)?;
    }
    Ok(())
}

pub fn polars_dataframe_to_arrow_record_batches(
    dataframe: &DataFrame,
    schema: Arc<Schema>,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let mut arrow_record_batches = Vec::with_capacity(10);
    for record_batch in dataframe.iter_chunks_physical() {
        let pl_arrays = record_batch.arrays();
        let mut arrow_arrays = Vec::with_capacity(pl_arrays.len());
        for pl_array in pl_arrays {
            let arrow_array: Arc<dyn arrow_array::Array>;
            let pl_datatype = pl_array.data_type();
            if *pl_datatype == polars_arrow::datatypes::ArrowDataType::Utf8View {
                let string_vec = pl_array
                    .as_any()
                    .downcast_ref::<polars_arrow::array::Utf8ViewArray>()
                    .ok_or(anyhow::anyhow!(
                        "Error downcast polars arrow array to Utf8ViewArray"
                    ))?
                    .into_iter()
                    .map(|e| e.unwrap())
                    .collect::<Vec<_>>();
                arrow_array = Arc::new(arrow_array::StringArray::from(string_vec));
            } else {
                arrow_array = convert_polars_arrow_array_to_arrow_array(
                    pl_array.clone(),
                    pl_datatype.clone().into(),
                )?;
            }
            arrow_arrays.push(arrow_array);
        }

        let record_batch_arrow =
            arrow::array::RecordBatch::try_new(schema.clone(), arrow_arrays)?;
        arrow_record_batches.push(record_batch_arrow);
    }
    Ok(arrow_record_batches)
}

// Converts a polars-arrow Arrow array to an arrow-rs Arrow array.
pub fn convert_polars_arrow_array_to_arrow_array(
    polars_array: Box<dyn polars_arrow::array::Array>,
    arrow_datatype: arrow_schema::DataType,
) -> std::result::Result<arrow_array::ArrayRef, arrow_schema::ArrowError> {
    let polars_c_array = polars_arrow::ffi::export_array_to_c(polars_array);
    // Safety: `polars_arrow::ffi::ArrowArray` has the same memory layout as `arrow::ffi::FFI_ArrowArray`.
    let arrow_c_array: arrow_data::ffi::FFI_ArrowArray =
        unsafe { std::mem::transmute(polars_c_array) };
    Ok(arrow_array::make_array(unsafe {
        arrow::ffi::from_ffi_and_data_type(arrow_c_array, arrow_datatype)
    }?))
}
