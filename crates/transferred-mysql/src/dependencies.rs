//! Verifies that replacement cannot move incoming foreign keys or discard triggers.

use crate::error::Result;
use mysql_async::Conn;
use mysql_async::prelude::Queryable as _;

use crate::connection::Connection;
use crate::table::Table;

pub(crate) async fn verify(conn: &mut Conn, connection: &Connection, table: &Table) -> Result<()> {
    foreign_keys(conn, connection, table).await?;
    trigger_privilege(conn, connection, table).await?;
    let count: Option<u64> = connection.wait(conn.exec_first(
        "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE BINARY TRIGGER_SCHEMA=BINARY ? AND BINARY EVENT_OBJECT_TABLE=BINARY ?",
        (&table.database, &table.name),
    )).await?;
    if count != Some(0) {
        return Err(format!("table {} has triggers; replacement is refused", table.sql()).into());
    }
    Ok(())
}

async fn foreign_keys(conn: &mut Conn, connection: &Connection, table: &Table) -> Result<()> {
    // MySQL decodes foreign-key REF_NAME to UTF-8, unlike INNODB_TABLES.NAME.
    let count: Option<u64> = connection.wait(conn.exec_first(
        "SELECT COUNT(*) FROM information_schema.INNODB_FOREIGN WHERE BINARY REF_NAME=BINARY CONCAT(?,'/',?)",
        (&table.database, &table.name),
    )).await.map_err(|error| format!("cannot verify incoming foreign keys for {} (PROCESS is required): {error}", table.sql()))?;
    if count != Some(0) {
        return Err(format!(
            "table {} has an incoming foreign key; replacement is refused",
            table.sql()
        )
        .into());
    }
    Ok(())
}

async fn trigger_privilege(conn: &mut Conn, connection: &Connection, table: &Table) -> Result<()> {
    let account: String = connection
        .wait(conn.query_first("SELECT CURRENT_USER()"))
        .await?
        .ok_or("missing account identity")?;
    let (user, host) = account.rsplit_once('@').ok_or("invalid account identity")?;
    let grantee = format!("'{user}'@'{host}'");
    let permission: Option<u8> = connection.wait(conn.exec_first(
        "SELECT 1 FROM information_schema.USER_PRIVILEGES WHERE BINARY GRANTEE=BINARY ? AND PRIVILEGE_TYPE='TRIGGER' \
         UNION ALL SELECT 1 FROM information_schema.SCHEMA_PRIVILEGES WHERE BINARY GRANTEE=BINARY ? AND BINARY TABLE_SCHEMA=BINARY ? AND PRIVILEGE_TYPE='TRIGGER' \
         UNION ALL SELECT 1 FROM information_schema.TABLE_PRIVILEGES WHERE BINARY GRANTEE=BINARY ? AND BINARY TABLE_SCHEMA=BINARY ? AND BINARY TABLE_NAME=BINARY ? AND PRIVILEGE_TYPE='TRIGGER' LIMIT 1",
        (&grantee, &grantee, &table.database, &grantee, &table.database, &table.name),
    )).await?;
    if permission != Some(1) {
        return Err("a direct TRIGGER grant is required to verify complete trigger metadata for an existing target".into());
    }
    Ok(())
}
