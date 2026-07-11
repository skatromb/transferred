//! Postgres `Type` → Arrow `DataType` mapping (v0 primitives).

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use tokio_postgres::Column;
use tokio_postgres::binary_copy::BinaryCopyOutRow;
use tokio_postgres::types::{FromSql, Type};
use transferred_core::{Result, TransferredError};

/// Derive an Arrow schema from Postgres column metadata. All fields nullable.
pub fn derive_schema(columns: &[Column]) -> Result<Arc<Schema>> {
    let fields = columns
        .iter()
        .map(|col| Ok(Field::new(col.name(), pg_to_arrow(col.type_())?, true)))
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(Schema::new(fields)))
}

/// Map a Postgres type to its Arrow equivalent; error on unsupported types.
fn pg_to_arrow(pg: &Type) -> Result<DataType> {
    Ok(match *pg {
        Type::BOOL => DataType::Boolean,
        Type::INT2 => DataType::Int16,
        Type::INT4 => DataType::Int32,
        Type::INT8 => DataType::Int64,
        Type::FLOAT4 => DataType::Float32,
        Type::FLOAT8 => DataType::Float64,
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => DataType::Utf8,
        Type::BYTEA => DataType::Binary,
        ref other => {
            return Err(TransferredError::source(format!(
                "Postgres type `{}` (oid {}) not supported in 0.1",
                other.name(),
                other.oid()
            )));
        }
    })
}

/// Build a `RecordBatch` from a chunk of PG rows matching `schema`.
pub fn rows_to_batch(schema: &Arc<Schema>, chunk: &[BinaryCopyOutRow]) -> Result<RecordBatch> {
    let arrays = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            Ok(match field.data_type() {
                DataType::Boolean => {
                    Arc::new(BooleanArray::from(col::<bool>(chunk, i))) as ArrayRef
                }
                DataType::Int16 => Arc::new(Int16Array::from(col::<i16>(chunk, i))) as ArrayRef,
                DataType::Int32 => Arc::new(Int32Array::from(col::<i32>(chunk, i))) as ArrayRef,
                DataType::Int64 => Arc::new(Int64Array::from(col::<i64>(chunk, i))) as ArrayRef,
                DataType::Float32 => Arc::new(Float32Array::from(col::<f32>(chunk, i))) as ArrayRef,
                DataType::Float64 => Arc::new(Float64Array::from(col::<f64>(chunk, i))) as ArrayRef,
                DataType::Utf8 => Arc::new(StringArray::from(col::<&str>(chunk, i))) as ArrayRef,
                DataType::Binary => Arc::new(BinaryArray::from(col::<&[u8]>(chunk, i))) as ArrayRef,
                other => {
                    return Err(TransferredError::source(format!(
                        "unsupported arrow type: {other}"
                    )));
                }
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(RecordBatch::try_new(schema.clone(), arrays)?)
}

/// Collect column `i` from every row, `None` for SQL NULL.
fn col<'a, T>(rows: &'a [BinaryCopyOutRow], i: usize) -> Vec<Option<T>>
where
    Option<T>: FromSql<'a>,
{
    rows.iter().map(|row| row.get(i)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every type `pg_to_arrow` accepts must have a matching arm in `rows_to_batch`.
    #[test]
    fn no_drift_between_schema_and_batch_conversion() {
        let mut drifted = Vec::new();

        for oid in 0..10_000 {
            let Some(pg) = Type::from_oid(oid) else {
                continue;
            };

            let Ok(data_type) = pg_to_arrow(&pg) else {
                continue;
            };

            let schema = Arc::new(Schema::new(vec![Field::new("c", data_type.clone(), true)]));

            if let Err(e) = rows_to_batch(&schema, &[]) {
                drifted.push(format!(
                    "`{}` maps to `{data_type}` but `rows_to_batch` can't build it: {e}",
                    pg.name()
                ));
            }
        }

        assert_eq!(drifted, Vec::<String>::new());
    }
}
