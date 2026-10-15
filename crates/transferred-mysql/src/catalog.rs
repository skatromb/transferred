//! MySQL table and column metadata.

use crate::error::Result;
use mysql_async::prelude::{FromValue, Queryable as _};
use mysql_async::{Conn, Row};

use crate::column::Column;
use crate::connection::Connection;
use crate::table::Table;

pub(crate) async fn inspect(
    conn: &mut Conn,
    connection: &Connection,
    table: &mut Table,
) -> Result<Vec<Column>> {
    normalize(conn, connection, table).await?;
    if description(conn, connection, table).await?.is_none() {
        return Err(format!("table {} does not exist", table.sql()).into());
    }
    columns(conn, connection, table).await
}

pub(crate) async fn normalize(
    conn: &mut Conn,
    connection: &Connection,
    table: &mut Table,
) -> Result<()> {
    let mode: Option<u8> = connection
        .wait(conn.query_first("SELECT @@lower_case_table_names"))
        .await?;
    if mode != Some(0) {
        let folded: Row = connection
            .wait(conn.exec_first(
                "SELECT LOWER(?) AS database_name, LOWER(?) AS table_name",
                (&table.database, &table.name),
            ))
            .await?
            .ok_or("missing normalized identifiers")?;
        table.database = cell(&folded, "database_name")?;
        table.name = cell(&folded, "table_name")?;
    }
    Ok(())
}

pub(crate) async fn description(
    conn: &mut Conn,
    connection: &Connection,
    table: &Table,
) -> Result<Option<String>> {
    let found: Option<Row> = connection.wait(conn.exec_first(
        "SELECT ENGINE, TABLE_TYPE, CREATE_OPTIONS FROM information_schema.TABLES WHERE BINARY TABLE_SCHEMA=BINARY ? AND BINARY TABLE_NAME=BINARY ?",
        (&table.database, &table.name),
    )).await?;
    let Some(row) = found else {
        return Ok(None);
    };
    ordinary_table(&row)?;
    let create: Row = connection
        .wait(conn.query_first(format!("SHOW CREATE TABLE {}", table.sql())))
        .await?
        .ok_or("table disappeared")?;
    Ok(Some(
        create
            .get_opt::<String, _>(1)
            .ok_or("missing table definition")??,
    ))
}

async fn columns(conn: &mut Conn, connection: &Connection, table: &Table) -> Result<Vec<Column>> {
    let checks: Vec<String> = connection.wait(conn.exec(
        "SELECT cc.CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS cc JOIN information_schema.TABLE_CONSTRAINTS tc ON cc.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA AND cc.CONSTRAINT_NAME=tc.CONSTRAINT_NAME WHERE BINARY tc.TABLE_SCHEMA=BINARY ? AND BINARY tc.TABLE_NAME=BINARY ? AND tc.CONSTRAINT_TYPE='CHECK' AND tc.ENFORCED='YES'",
        (&table.database, &table.name),
    )).await?;
    let rows: Vec<Row> = connection.wait(conn.exec(
        "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH, COLUMN_COMMENT FROM information_schema.COLUMNS WHERE BINARY TABLE_SCHEMA=BINARY ? AND BINARY TABLE_NAME=BINARY ? ORDER BY ORDINAL_POSITION",
        (&table.database, &table.name),
    )).await?;
    if rows.is_empty() {
        return Err("table has no visible columns".into());
    }
    rows.iter()
        .map(|row| Column::from_catalog(row, &checks))
        .collect()
}

pub(crate) fn cell<Native: FromValue>(row: &Row, name: &str) -> Result<Native> {
    Ok(row
        .get_opt(name)
        .ok_or_else(|| format!("missing catalog field {name}"))??)
}

fn ordinary_table(row: &Row) -> Result<()> {
    let engine: Option<String> = cell(row, "ENGINE")?;
    let kind: String = cell(row, "TABLE_TYPE")?;
    let options: String = cell(row, "CREATE_OPTIONS")?;
    if engine.as_deref() != Some("InnoDB")
        || kind != "BASE TABLE"
        || options.contains("partitioned")
    {
        return Err("only ordinary, non-partitioned InnoDB base tables are supported".into());
    }
    Ok(())
}
