use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, RecordBatch};
use arrow::buffer::{Buffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};

use super::Encoder;
use crate::connection::MAX_PACKET;

#[test]
fn rejects_oversized_values_before_binding() {
    let batch = wide_batch(MAX_PACKET.saturating_add(1), 1);
    let encoder = Encoder::new(batch.schema()).unwrap();
    let error = encoder.row(&batch, 0).err().unwrap().to_string();
    assert!(error.contains("packet limit"), "{error}");
}

#[test]
fn stops_binding_when_row_exceeds_client_limit() {
    let batch = wide_batch(MAX_PACKET.saturating_div(2), 3);
    let encoder = Encoder::new(batch.schema()).unwrap();
    let error = encoder.row(&batch, 0).err().unwrap().to_string();
    assert!(error.contains("packet limit"), "{error}");
}

fn wide_batch(bytes: usize, columns: usize) -> RecordBatch {
    let offsets = OffsetBuffer::new(vec![0, i32::try_from(bytes).unwrap()].into());
    let array: ArrayRef = Arc::new(BinaryArray::new(
        offsets,
        Buffer::from(vec![b'x'; bytes]),
        None,
    ));
    let fields: Vec<_> = (0..columns)
        .map(|column| Field::new(format!("blob_{column}"), DataType::Binary, true))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![array; columns]).unwrap()
}
