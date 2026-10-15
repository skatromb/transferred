//! MySQL connection validation and isolated session settings.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use mysql_async::prelude::Queryable as _;
use mysql_async::{Conn, Opts, OptsBuilder, SslOpts};
use percent_encoding::percent_decode_str;
use tokio::time::timeout;
use url::Url;

use crate::error::Result;

/// Hard bound on an incoming protocol message, also respected by the writer.
pub(crate) const MAX_PACKET: usize = 64 * 1024 * 1024;

pub(crate) struct Connection {
    opts: Opts,
    database: String,
    connect_timeout: Duration,
    io_timeout: Duration,
    lock_timeout: u32,
}

impl Connection {
    pub(crate) fn database(&self) -> &str {
        &self.database
    }

    pub(crate) fn parse(dsn: &str) -> Result<Self> {
        let url = Url::parse(dsn)?;
        validate_url(&url)?;
        let database = decoded(url.path().trim_start_matches('/'))?;
        if database.is_empty() || database.contains(['/', '\0']) {
            return Err("a MySQL URL must name one database".into());
        }
        let settings = parameters(&url)?;
        let opts = options(&url, &database)?.ssl_opts(tls(&settings)?).into();
        Ok(Self {
            opts,
            database,
            connect_timeout: seconds(&settings, "connect_timeout", 10)?,
            io_timeout: seconds(&settings, "io_timeout", 60)?,
            lock_timeout: u32::try_from(seconds(&settings, "lock_timeout", 10)?.as_secs())?,
        })
    }

    pub(crate) async fn open(&self) -> Result<Conn> {
        let mut conn = timeout(self.connect_timeout, Conn::new(self.opts.clone())).await??;
        let version: Option<String> = self.wait(conn.query_first("SELECT VERSION()")).await?;
        if version
            .as_deref()
            .is_none_or(|banner| banner.contains("MariaDB"))
            || conn.server_version() < (8, 4, 0)
        {
            return Err("requires MySQL 8.4 or newer, not MariaDB".into());
        }
        self.initialize(&mut conn).await?;
        Ok(conn)
    }

    pub(crate) async fn wait<Output, Operation>(&self, operation: Operation) -> Result<Output>
    where
        Operation: Future<Output = mysql_async::Result<Output>>,
    {
        Ok(timeout(self.io_timeout, operation).await??)
    }

    async fn initialize(&self, conn: &mut Conn) -> Result<()> {
        self.wait(conn.query_drop(format!(
            "SET SESSION sql_mode='STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO', \
             time_zone='+00:00', autocommit=1, transaction_isolation='REPEATABLE-READ', \
             lock_wait_timeout={}", self.lock_timeout
        ))).await?;
        self.wait(conn.query_drop("SET NAMES utf8mb4 COLLATE utf8mb4_bin"))
            .await
    }
}

fn validate_url(url: &Url) -> Result<()> {
    if url.scheme() != "mysql" || url.host_str().is_none() || url.fragment().is_some() {
        return Err("expected mysql://user:password@host/database without a fragment".into());
    }
    Ok(())
}

fn options(url: &Url, database: &str) -> Result<OptsBuilder> {
    let host = url
        .host_str()
        .ok_or("missing MySQL host")?
        .trim_matches(['[', ']']);
    Ok(OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(url.port().unwrap_or(3306))
        .user(Some(decoded(url.username())?))
        .pass(url.password().map(decoded).transpose()?)
        .db_name(Some(database))
        .prefer_socket(false)
        .max_allowed_packet(Some(MAX_PACKET)))
}

type UrlOptions = BTreeMap<String, String>;

fn parameters(url: &Url) -> Result<UrlOptions> {
    let mut settings = BTreeMap::new();
    for (key, argument) in url.query_pairs() {
        if !matches!(
            key.as_ref(),
            "tls" | "sslrootcert" | "connect_timeout" | "io_timeout" | "lock_timeout"
        ) {
            return Err(format!("unknown connection parameter {key}").into());
        }
        if settings
            .insert(key.to_string(), argument.into_owned())
            .is_some()
        {
            return Err(format!("duplicate connection parameter {key}").into());
        }
    }
    Ok(settings)
}

fn tls(settings: &UrlOptions) -> Result<Option<SslOpts>> {
    match settings.get("tls").map_or("verify-full", String::as_str) {
        "disabled" if !settings.contains_key("sslrootcert") => Ok(None),
        "verify-full" => {
            let certs = settings
                .get("sslrootcert")
                .map(|path| PathBuf::from(path).into())
                .into_iter()
                .collect();
            Ok(Some(SslOpts::default().with_root_certs(certs)))
        }
        _ => Err("tls must be verify-full or disabled; sslrootcert requires TLS".into()),
    }
}

fn seconds(settings: &UrlOptions, name: &str, default: u32) -> Result<Duration> {
    let seconds = settings
        .get(name)
        .map(|number| number.parse::<u32>())
        .transpose()?
        .unwrap_or(default);
    if seconds == 0 {
        return Err(format!("{name} must be positive seconds").into());
    }
    Ok(Duration::from_secs(u64::from(seconds)))
}

fn decoded(text: &str) -> Result<String> {
    Ok(percent_decode_str(text).decode_utf8()?.into_owned())
}
