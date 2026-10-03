//! Postgres binary COPY: the wire format the source reads rows from.

use bytes::{Buf as _, Bytes};
use futures::{Stream, TryStreamExt as _, future};
use tokio_postgres::CopyOutStream;
use transferred_core::{Result, TransferredError};

use crate::{COPY_HEADER, COPY_TRAILER, NULL_FIELD};

/// A value's bytes exactly as Postgres sent them; `None` where it sent none.
pub(crate) type Cell<'row> = Option<&'row [u8]>;

/// Each row of a binary COPY as its own bytes, which Postgres sends one per message.
pub(crate) fn rows(copy: CopyOutStream) -> impl Stream<Item = Result<Bytes>> {
    copy.map_err(TransferredError::in_source)
        .try_filter_map(|message| future::ready(Ok(row(message))))
}

/// The row a message carries, past the header opening the first; `None` for the closing trailer.
/// A row opens with its field count, a small positive number, so neither prefix can match it.
fn row(mut message: Bytes) -> Option<Bytes> {
    if message.starts_with(COPY_HEADER) {
        message.advance(COPY_HEADER.len());
    }

    (!message.starts_with(&COPY_TRAILER.to_be_bytes())).then_some(message)
}

/// Reads one row's cells, in column order.
pub(crate) struct Cells<'row>(&'row [u8]);

impl<'row> Cells<'row> {
    /// Starts on a row, checking it holds a cell for each of `width` columns.
    pub(crate) fn new(mut row: &'row [u8], width: usize) -> Result<Self> {
        let count = row.try_get_i16().map_err(TransferredError::in_source)?;
        if usize::try_from(count) != Ok(width) {
            return Err(TransferredError::in_source(format!(
                "a COPY row holds {count} fields, not {width}"
            )));
        }

        Ok(Self(row))
    }

    /// Takes the next cell off the row.
    pub(crate) fn take(&mut self) -> Result<Cell<'row>> {
        let len = self.0.try_get_i32().map_err(TransferredError::in_source)?;
        if len == NULL_FIELD {
            return Ok(None);
        }

        let cell = usize::try_from(len)
            .ok()
            .and_then(|bytes| self.0.split_off(..bytes));

        cell.map(Some).ok_or_else(|| {
            TransferredError::in_source(format!("a COPY field of {len} bytes runs past its row"))
        })
    }
}
