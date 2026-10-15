//! Validates a batch schema and binds rows in declared column order.

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use mysql_async::Value;

use crate::connection::MAX_PACKET;
use crate::error::Result;

use super::encoding::Encoding;
use crate::table::{quote, validate};

pub(crate) struct Encoder {
    schema: SchemaRef,
    encodings: Vec<Encoding>,
    declarations: String,
}

impl Encoder {
    pub(crate) fn new(schema: SchemaRef) -> Result<Self> {
        if schema.fields().is_empty() {
            return Err("a table requires at least one typed column".into());
        }
        let encodings = encodings(&schema)?;
        let declarations = declarations(&schema, &encodings)?;
        Ok(Self {
            schema,
            encodings,
            declarations,
        })
    }

    pub(crate) fn declarations(&self) -> &str {
        &self.declarations
    }

    pub(crate) const fn width(&self) -> usize {
        self.encodings.len()
    }

    pub(crate) fn check(&self, batch: &RecordBatch) -> Result<()> {
        if batch.schema().fields() != self.schema.fields() {
            return Err("batch schema changed during the transfer".into());
        }
        Ok(())
    }

    pub(crate) fn row(&self, batch: &RecordBatch, row: usize) -> Result<Vec<Value>> {
        let mut size = 0_u64;
        let row_number = row.saturating_add(1);
        self.encodings
            .iter()
            .zip(batch.columns())
            .zip(self.schema.fields())
            .map(|((encoding, array), field)| {
                let name = field.name();
                let parameter = encoding
                    .parameter(array.as_ref(), row)
                    .map_err(|error| format!("column {name}, batch row {row_number}: {error}"))?;
                size = size.saturating_add(parameter.bin_len());
                if size > u64::try_from(MAX_PACKET)? {
                    return Err(format!("row {row_number}: packet limit exceeded").into());
                }
                Ok(parameter)
            })
            .collect()
    }
}

fn encodings(schema: &SchemaRef) -> Result<Vec<Encoding>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            validate(field.name())?;
            Encoding::new(field).map_err(|error| {
                format!("column {} ({}): {error}", field.name(), field.data_type()).into()
            })
        })
        .collect()
}

fn declarations(schema: &SchemaRef, encodings: &[Encoding]) -> Result<String> {
    let columns = schema
        .fields()
        .iter()
        .zip(encodings)
        .map(|(field, encoding)| {
            let sql_type = encoding.declaration(field.name()).map_err(|error| {
                format!("column {} ({}): {error}", field.name(), field.data_type())
            })?;
            Ok(format!("{} {sql_type}", quote(field.name())))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(columns.join(","))
}

#[cfg(test)]
mod tests;
