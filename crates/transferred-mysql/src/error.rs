//! Keeps protocol errors intact until the source or destination boundary.

use std::result;

use transferred_core::{AnyError, TransferredError};

pub(crate) type Result<Output> = result::Result<Output, AnyError>;

pub(crate) fn source_context(error: TransferredError, table: &str) -> TransferredError {
    if let TransferredError::Source(cause) = error {
        TransferredError::in_source(format!("table {table}: {cause}"))
    } else {
        error
    }
}

pub(crate) fn destination_context(error: TransferredError, table: &str) -> TransferredError {
    if let TransferredError::Destination(cause) = error {
        TransferredError::in_destination(format!("table {table}: {cause}"))
    } else {
        error
    }
}
