//! Validates MySQL arguments without network access.
#![cfg(test)]

use transferred_mysql::{MySqlDestination, MySqlSource};

#[test]
fn constructs_without_contacting_the_host() {
    let dsn = "mysql://user:secret@unreachable.invalid:3306/database";
    assert!(MySqlSource::new(dsn, "table").is_ok());
    assert!(MySqlDestination::new(dsn, "`db.name`.`table``name`").is_ok());
}

#[test]
fn rejects_other_schemes_options_and_expressions() {
    for dsn in [
        "mariadb://host/database",
        "postgres://host/database",
        "mysql://host/",
        "mysql://host/database?tls=preferred",
        "mysql://host/database?io_timeout=0",
        "mysql://host/database?unknown=true",
        "mysql://host/database?tls=disabled&tls=disabled",
    ] {
        assert!(MySqlSource::new(dsn, "table").is_err(), "{dsn}");
        assert!(MySqlDestination::new(dsn, "table").is_err(), "{dsn}");
    }
    for table in ["", "db..table", "table; SELECT 1", "`unterminated"] {
        assert!(
            MySqlSource::new("mysql://host/database", table).is_err(),
            "{table}"
        );
    }
}
