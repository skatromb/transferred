//! Sends bounded prepared multirow INSERT statements without per-row execution.

use std::mem;

use arrow::array::RecordBatch;
use futures::TryStreamExt as _;
use mysql_async::prelude::Queryable as _;
use mysql_async::{Conn, Statement, Value};
use transferred_core::{BatchStream, Result as TransferResult, TransferredError};

use super::encoder::Encoder;
use crate::connection::{Connection, MAX_PACKET};
use crate::error::Result;
use crate::staging::Staging;

const PORTION_ROWS: usize = 1_000;
const PORTION_BYTES: u64 = 1024 * 1024;
/// Maximum parameter count in the protocol's 16-bit prepared-statement field.
const MAX_PARAMETERS: usize = 0xffff;

pub(crate) struct Writer<'session> {
    conn: &'session mut Conn,
    connection: &'session Connection,
    encoder: Encoder,
    staging: &'session Staging,
    statement: Option<Statement>,
    packet_limit: u64,
    header_bytes: u64,
    row_limit: usize,
}

impl<'session> Writer<'session> {
    pub(crate) async fn new(
        conn: &'session mut Conn,
        connection: &'session Connection,
        encoder: Encoder,
        staging: &'session Staging,
    ) -> Result<Self> {
        let packet_limit = packet_limit(conn, connection).await?;
        let header_bytes =
            u64::try_from(staging.insert(encoder.width(), 1).len())?.saturating_add(512);
        let row_limit = row_limit(encoder.width())?;
        Ok(Self {
            conn,
            connection,
            encoder,
            staging,
            statement: None,
            packet_limit,
            header_bytes,
            row_limit,
        })
    }

    pub(crate) async fn load(mut self, mut batches: BatchStream) -> TransferResult<u64> {
        let mut count = 0_u64;
        while let Some(batch) = batches.try_next().await? {
            self.write(&batch)
                .await
                .map_err(TransferredError::in_destination)?;
            count = count
                .checked_add(
                    u64::try_from(batch.num_rows()).map_err(TransferredError::in_destination)?,
                )
                .ok_or_else(|| TransferredError::in_destination("row count overflow"))?;
        }
        if let Some(statement) = self.statement {
            self.connection
                .wait(self.conn.close(statement))
                .await
                .map_err(TransferredError::in_destination)?;
        }
        Ok(count)
    }

    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.encoder.check(batch)?;
        let mut portion = Vec::new();
        let mut size = self.header_bytes;
        for row in 0..batch.num_rows() {
            let bindings = self.encoder.row(batch, row)?;
            let row_size = row_bytes(&bindings);
            self.check_row_size(row_size)?;
            if !portion.is_empty()
                && (portion.len() >= self.row_limit
                    || size.saturating_add(row_size) > self.packet_limit.min(PORTION_BYTES))
            {
                self.send(mem::take(&mut portion)).await?;
                size = self.header_bytes;
            }
            size = size.saturating_add(row_size);
            portion.push(bindings);
        }
        self.send(portion).await
    }

    async fn send(&mut self, portion: Vec<Vec<Value>>) -> Result<()> {
        if portion.is_empty() {
            return Ok(());
        }
        let expected = u64::try_from(portion.len())?;
        let statement = self.prepare(portion.len()).await?;
        let bindings: Vec<Value> = portion.into_iter().flatten().collect();
        self.connection
            .wait(self.conn.exec_drop(statement, bindings))
            .await?;
        if self.conn.affected_rows() != expected || self.conn.get_warnings() != 0 {
            return Err(format!(
                "multirow INSERT expected {expected} rows, server reported {} rows and {} warnings",
                self.conn.affected_rows(),
                self.conn.get_warnings()
            )
            .into());
        }
        Ok(())
    }

    async fn prepare(&mut self, rows: usize) -> Result<Statement> {
        let parameter_count = rows
            .checked_mul(self.encoder.width())
            .ok_or("parameter count overflow")?;
        if let Some(statement) = &self.statement
            && usize::from(statement.num_params()) == parameter_count
        {
            return Ok(statement.clone());
        }
        if let Some(statement) = self.statement.take() {
            self.connection.wait(self.conn.close(statement)).await?;
        }
        let query = self.staging.insert(self.encoder.width(), rows);
        let statement = self.connection.wait(self.conn.prep(query)).await?;
        self.statement = Some(statement.clone());
        Ok(statement)
    }

    fn check_row_size(&self, row_size: u64) -> Result<()> {
        if row_size.saturating_add(self.header_bytes) > self.packet_limit {
            return Err(format!(
                "encoded row exceeds the packet limit of {} bytes",
                self.packet_limit
            )
            .into());
        }
        Ok(())
    }
}

async fn packet_limit(conn: &mut Conn, connection: &Connection) -> Result<u64> {
    let packet: u64 = connection
        .wait(conn.query_first("SELECT @@max_allowed_packet"))
        .await?
        .ok_or("missing packet limit")?;
    Ok(packet.min(u64::try_from(MAX_PACKET)?))
}

fn row_limit(width: usize) -> Result<usize> {
    MAX_PARAMETERS
        .checked_div(width)
        .filter(|limit| *limit > 0)
        .map(|limit| limit.min(PORTION_ROWS))
        .ok_or_else(|| "column count exceeds the prepared parameter limit".into())
}

fn row_bytes(bindings: &[Value]) -> u64 {
    // Three bytes per cell cover its type/null flags and its SQL placeholder.
    bindings.iter().fold(3_u64, |bytes, cell| {
        bytes.saturating_add(cell.bin_len()).saturating_add(3)
    })
}
