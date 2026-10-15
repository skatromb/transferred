//! Owns one load's staging table and atomic publication.

use std::fmt::Write as _;

use log::warn;
use mysql_async::prelude::Queryable as _;
use mysql_async::{Conn, Error as ClientError, Value};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::connection::Connection;
use crate::error::Result;
use crate::table::Table;
use crate::{catalog, dependencies};

pub(crate) struct Staging {
    target: Table,
    stage: Table,
    backup: Table,
    original: Option<String>,
    comment: String,
}

impl Staging {
    pub(crate) async fn create(
        conn: &mut Conn,
        connection: &Connection,
        mut target: Table,
        declarations: &str,
    ) -> Result<Self> {
        lock(conn, connection, &mut target).await?;
        let original = catalog::description(conn, connection, &target).await?;
        if original.is_some() {
            dependencies::verify(conn, connection, &target).await?;
        }
        let staging = Self::names(target, original);
        staging.create_table(conn, connection, declarations).await?;
        Ok(staging)
    }

    pub(crate) fn insert(&self, columns: usize, rows: usize) -> String {
        let tuple = format!("({})", vec!["?"; columns].join(","));
        let tuples = vec![tuple; rows].join(",");
        format!("INSERT INTO {} VALUES {tuples}", self.stage.sql())
    }

    pub(crate) async fn recheck(&self, conn: &mut Conn, connection: &Connection) -> Result<()> {
        let current = catalog::description(conn, connection, &self.target).await?;
        if current != self.original {
            return Err(format!("target {} changed while loading", self.target.sql()).into());
        }
        if current.is_some() {
            dependencies::verify(conn, connection, &self.target).await?;
        }
        Ok(())
    }

    pub(crate) async fn publish(&self, conn: &mut Conn, connection: &Connection) -> Result<String> {
        let rename = self.rename();
        if let Err(error) = connection.wait(conn.query_drop(rename)).await {
            if error
                .downcast_ref::<ClientError>()
                .is_some_and(|failure| matches!(failure, ClientError::Server(_)))
            {
                self.cleanup(conn, connection).await;
                return Err(error);
            }
            return Err(format!("publication outcome unknown for {}; do not retry or clean up automatically; staging={}, backup={}: {error}",
                self.target.sql(), self.stage.sql(), self.backup.sql()).into());
        }
        if self.original.is_some() {
            self.drop_backup(conn, connection).await;
        }
        Ok(self.target.sql())
    }

    pub(crate) async fn cleanup(&self, conn: &mut Conn, connection: &Connection) {
        if let Err(error) = self.drop_owned_stage(conn, connection).await {
            warn!(target: "mysql.destination", "staging {} was not removed: {error}", self.stage.sql());
        }
    }

    fn names(target: Table, original: Option<String>) -> Self {
        let identifier = Uuid::new_v4().simple().to_string();
        let stage = Table {
            database: target.database.clone(),
            name: format!("_transferred_{identifier}_s"),
        };
        let backup = Table {
            database: target.database.clone(),
            name: format!("_transferred_{identifier}_b"),
        };
        let comment = format!("transferred:run={identifier};target={}", target.sql());
        Self {
            target,
            stage,
            backup,
            original,
            comment,
        }
    }

    async fn create_table(
        &self,
        conn: &mut Conn,
        connection: &Connection,
        declarations: &str,
    ) -> Result<()> {
        let comment = Value::Bytes(self.comment.as_bytes().to_vec()).as_sql(false);
        connection.wait(conn.query_drop(format!(
            "CREATE TABLE {} ({declarations}) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin COMMENT={comment}", self.stage.sql()
        ))).await.map_err(|error| format!("could not create staging {}: {error}", self.stage.sql()).into())
    }

    fn rename(&self) -> String {
        if self.original.is_some() {
            format!(
                "RENAME TABLE {} TO {}, {} TO {}",
                self.target.sql(),
                self.backup.sql(),
                self.stage.sql(),
                self.target.sql()
            )
        } else {
            format!("RENAME TABLE {} TO {}", self.stage.sql(), self.target.sql())
        }
    }

    async fn drop_owned_stage(&self, conn: &mut Conn, connection: &Connection) -> Result<()> {
        let comment: Option<String> = connection.wait(conn.exec_first(
            "SELECT TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
            (&self.stage.database, &self.stage.name),
        )).await?;
        if comment.as_deref() != Some(&self.comment) {
            return Err("staging ownership cannot be verified".into());
        }
        connection
            .wait(conn.query_drop(format!("DROP TABLE {}", self.stage.sql())))
            .await
    }

    async fn drop_backup(&self, conn: &mut Conn, connection: &Connection) {
        if let Err(error) = connection
            .wait(conn.query_drop(format!("DROP TABLE {}", self.backup.sql())))
            .await
        {
            warn!(target: "mysql.destination", "load published; backup {} was not removed: {error}", self.backup.sql());
        }
    }
}

async fn lock(conn: &mut Conn, connection: &Connection, table: &mut Table) -> Result<()> {
    catalog::normalize(conn, connection, table).await?;
    let mut name = String::from("transferred:");
    // A 12-byte prefix and 26 digest bytes fit a 64-byte lock name.
    for byte in Sha256::digest(table.sql().as_bytes()).iter().take(26) {
        write!(name, "{byte:02x}")?;
    }
    let acquired: Option<u8> = connection
        .wait(conn.exec_first("SELECT GET_LOCK(?, 0)", (name,)))
        .await?;
    if acquired != Some(1) {
        return Err(format!("target {} is busy with another transfer", table.sql()).into());
    }
    Ok(())
}
