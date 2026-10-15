//! Collects decoded columns into bounded, schema-bearing Arrow batches.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use mysql_async::{Row, Value};

use super::column_decoder::ColumnDecoder;
use crate::column::Column;
use crate::error::Result;

const BATCH_ROWS: usize = 10_000;
const BATCH_BYTES: usize = 8 * 1024 * 1024;

pub(crate) struct Decoder {
    schema: SchemaRef,
    columns: Vec<ColumnDecoder>,
    rows: usize,
    bytes: usize,
    emitted: bool,
}

impl Decoder {
    pub(crate) fn new(columns: &[Column]) -> Self {
        let fields: Vec<Field> = columns
            .iter()
            .map(|column| column.field().clone())
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let capacity = row_capacity(&schema);
        let decoders = schema
            .fields()
            .iter()
            .cloned()
            .map(|field| ColumnDecoder::new(field, capacity))
            .collect();
        Self {
            schema,
            columns: decoders,
            rows: 0,
            bytes: 0,
            emitted: false,
        }
    }

    pub(crate) fn append(&mut self, row: &Row) -> Result<Option<RecordBatch>> {
        self.append_columns(row)?;
        self.rows = self.rows.saturating_add(1);
        self.bytes = self.bytes.saturating_add(row_bytes(row, &self.schema));
        if self.rows >= BATCH_ROWS || self.bytes >= BATCH_BYTES {
            self.flush().map(Some)
        } else {
            Ok(None)
        }
    }

    pub(crate) fn finish(&mut self) -> Result<Option<RecordBatch>> {
        if self.rows == 0 && self.emitted {
            Ok(None)
        } else {
            self.flush().map(Some)
        }
    }

    fn append_columns(&mut self, row: &Row) -> Result<()> {
        if row.len() != self.columns.len() {
            return Err("row width changed during SELECT".into());
        }
        for (index, column) in self.columns.iter_mut().enumerate() {
            column.append(row.as_ref(index).ok_or("missing row cell")?)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<RecordBatch> {
        let arrays = self.columns.iter_mut().map(ColumnDecoder::finish).collect();
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
        self.rows = 0;
        self.bytes = 0;
        self.emitted = true;
        Ok(batch)
    }
}

fn row_capacity(schema: &Schema) -> usize {
    let width: usize = schema
        .fields()
        .iter()
        .map(|field| {
            field
                .data_type()
                .primitive_width()
                .unwrap_or(16)
                .saturating_add(1)
        })
        .sum();
    // Reserves the row that crosses the limit so builders do not double their allocations.
    BATCH_BYTES.div_ceil(width.max(1)).clamp(1, BATCH_ROWS)
}

fn row_bytes(row: &Row, schema: &Schema) -> usize {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let storage = field.data_type().primitive_width().unwrap_or(16);
            let payload = if let Some(Value::Bytes(bytes)) = row.as_ref(index) {
                bytes.len()
            } else {
                0
            };
            storage.saturating_add(payload).saturating_add(1)
        })
        .sum()
}
