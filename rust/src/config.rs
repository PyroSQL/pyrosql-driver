//! Connection configuration for the PyroSQL client.

use crate::error::ClientError;

/// SQL syntax mode for the connection session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyntaxMode {
    /// PyroSQL native syntax (default — accepts PG syntax too).
    PyroSQL,
    /// Strict PostgreSQL compatibility mode.
    PostgreSQL,
    /// MySQL compatibility mode.
    MySQL,
}

impl SyntaxMode {
    /// Return the SQL `SET` value string for this mode.
    #[inline]
    pub fn as_set_value(&self) -> &'static str {
        match self {
            Self::PyroSQL => "pyrosql",
            Self::PostgreSQL => "postgresql",
            Self::MySQL => "mysql",
        }
    }
}

/// The wire protocol / transport scheme to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// PWire binary protocol over TCP. URL: `vsql://` or `vsqlw://`.
    Wire,
    /// PyroLink QUIC protocol (port 12520). URL: `quic://`.
    Quic,
    /// Auto-detect best transport.
    Auto,
    /// Unix domain socket (PG wire protocol).
    Unix,
    /// PostgreSQL wire protocol over TCP.
    Postgres,
    /// MySQL wire protocol over TCP.
    MySQL,
}

/// Configuration for connecting to a PyroSQL server.
///
/// Supports multiple connection schemes:
/// - `vsql://` — PyroLink QUIC protocol (port 12520)
/// - `postgres://` / `pg://` — PostgreSQL wire protocol (port 5432)
/// - `mysql://` — MySQL wire protocol (port 3306)
/// - `unix://` — PG wire over Unix domain socket
/// - `auto://` — auto-detect best transport
///
/// # Example
///
/// ```
/// use pyrosql::ConnectConfig;
///
/// // PyroLink QUIC (fastest for remote)
/// let cfg = ConnectConfig::from_url("vsql://admin:secret@localhost:12520/mydb").unwrap();
///
/// // PostgreSQL wire protocol (compatible with any PG tool)
/// let cfg = ConnectConfig::from_url("postgres://admin:secret@localhost:5432/mydb").unwrap();
///
/// // MySQL wire protocol
/// let cfg = ConnectConfig::from_url("mysql://root:pass@localhost:3306/mydb").unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct ConnectConfig {
    /// Connection scheme (determines wire protocol).
    pub scheme: Scheme,
    /// Server hostname or IP address.
    pub host: String,
    /// Server port (default depends on scheme).
    pub port: u16,
    /// Database name.
    pub database: String,
    /// Username for authentication.
    pub user: String,
    /// Password for authentication.
    pub password: String,
    /// Skip TLS server certificate verification (for development/testing only).
    pub tls_skip_verify: bool,
    /// Optional Unix socket path for direct same-host connection.
    /// When set, the client will attempt T1:Unix transport before QUIC.
    pub unix_socket_path: Option<String>,
    /// SQL syntax mode to set on the session after connecting.
    /// None = use server default (PyroSQL for QUIC, PostgreSQL for PG wire, MySQL for MySQL wire).
    pub syntax_mode: Option<SyntaxMode>,
}

impl ConnectConfig {
    /// Create a new config with host and port; other fields use defaults.
    #[must_use]
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            scheme: Scheme::Quic,
            host: host.to_owned(),
            port,
            database: String::new(),
            user: String::new(),
            password: String::new(),
            tls_skip_verify: false,
            unix_socket_path: None,
            syntax_mode: None,
        }
    }

    /// Set the Unix socket path for T1 transport.
    #[must_use]
    pub fn unix_socket(mut self, path: &str) -> Self {
        self.unix_socket_path = Some(path.to_owned());
        self
    }

    /// Set the database name.
    #[must_use]
    pub fn database(mut self, db: &str) -> Self {
        self.database = db.to_owned();
        self
    }

    /// Set the username.
    #[must_use]
    pub fn user(mut self, user: &str) -> Self {
        self.user = user.to_owned();
        self
    }

    /// Set the password.
    #[must_use]
    pub fn password(mut self, password: &str) -> Self {
        self.password = password.to_owned();
        self
    }

    /// Enable or disable TLS server certificate verification skipping.
    #[must_use]
    pub fn tls_skip_verify(mut self, skip: bool) -> Self {
        self.tls_skip_verify = skip;
        self
    }

    /// Set the SQL syntax mode for the session.
    #[must_use]
    pub fn syntax_mode(mut self, mode: SyntaxMode) -> Self {
        self.syntax_mode = Some(mode);
        self
    }

    /// Short alias for [`syntax_mode`](Self::syntax_mode). Added so driver
    /// users can write `.dialect(SyntaxMode::PostgreSQL)` in parity with
    /// the CLI's `-D pg` flag and the URL `?dialect=pg` query param.
    #[must_use]
    pub fn dialect(self, mode: SyntaxMode) -> Self {
        self.syntax_mode(mode)
    }

    /// Set the connection scheme.
    #[must_use]
    pub fn scheme(mut self, scheme: Scheme) -> Self {
        self.scheme = scheme;
        self
    }

    /// Parse a connection URL.
    ///
    /// Supported schemes:
    /// - `vsql://user:pass@host:port/database` — PyroLink QUIC transport (port 12520)
    /// - `postgres://user:pass@host:port/database` — PostgreSQL wire protocol (port 5432)
    /// - `pg://user:pass@host:port/database` — alias for postgres://
    /// - `mysql://user:pass@host:port/database` — MySQL wire protocol (port 3306)
    /// - `unix:///path/to/socket?database=mydb&user=admin` — PG wire over Unix socket
    /// - `auto://user:pass@host:port/database` — auto-detect best transport
    ///
    /// Port defaults to scheme-appropriate port if omitted.
    /// User, password, and database are optional.
    /// Append `?syntax_mode=mysql` (or `postgresql`, `pyrosql`) to override syntax.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::InvalidUrl`] if the URL cannot be parsed.
    pub fn from_url(url: &str) -> Result<Self, ClientError> {
        // Unix socket URL: unix:///path/to/socket?database=mydb&user=admin
        if let Some(rest) = url.strip_prefix("unix://") {
            let (path, query) = if let Some(q_pos) = rest.find('?') {
                (&rest[..q_pos], &rest[q_pos + 1..])
            } else {
                (rest, "")
            };

            if path.is_empty() {
                return Err(ClientError::InvalidUrl("unix:// URL requires a socket path".into()));
            }

            let mut cfg = Self {
                scheme: Scheme::Unix,
                host: "localhost".to_owned(),
                port: 5432,
                database: String::new(),
                user: String::new(),
                password: String::new(),
                tls_skip_verify: true, // no TLS over Unix socket
                unix_socket_path: Some(path.to_owned()),
                syntax_mode: None,
            };

            // Parse query parameters
            for param in query.split('&') {
                if param.is_empty() {
                    continue;
                }
                if let Some((key, value)) = param.split_once('=') {
                    match key {
                        "database" | "db" => cfg.database = value.to_owned(),
                        "user" => cfg.user = value.to_owned(),
                        "password" => cfg.password = value.to_owned(),
                        "syntax_mode" | "dialect" => {
                            cfg.syntax_mode = parse_syntax_mode(value)
                        }
                        _ => {} // ignore unknown params
                    }
                }
            }

            return Ok(cfg);
        }

        // Detect scheme and default port
        let (scheme, rest, default_port) = if let Some(r) = url.strip_prefix("vsqlw://") {
            (Scheme::Wire, r, 12520u16)
        } else if let Some(r) = url.strip_prefix("vsql://") {
            (Scheme::Wire, r, 12520u16)
        } else if let Some(r) = url.strip_prefix("quic://") {
            (Scheme::Quic, r, 12520u16)
        } else if let Some(r) = url.strip_prefix("auto://") {
            (Scheme::Auto, r, 12520)
        } else if let Some(r) = url.strip_prefix("postgres://") {
            (Scheme::Postgres, r, 5432)
        } else if let Some(r) = url.strip_prefix("postgresql://") {
            (Scheme::Postgres, r, 5432)
        } else if let Some(r) = url.strip_prefix("pg://") {
            (Scheme::Postgres, r, 5432)
        } else if let Some(r) = url.strip_prefix("mysql://") {
            (Scheme::MySQL, r, 3306)
        } else {
            return Err(ClientError::InvalidUrl(
                "URL must start with vsql://, postgres://, pg://, mysql://, unix://, or auto:// (quic:// also accepted)".into(),
            ));
        };

        // Split userinfo from host
        let (userinfo, hostpath) = if let Some(at_pos) = rest.rfind('@') {
            (&rest[..at_pos], &rest[at_pos + 1..])
        } else {
            ("", rest)
        };

        // Parse user:password
        let (user, password) = if userinfo.is_empty() {
            (String::new(), String::new())
        } else if let Some(colon) = userinfo.find(':') {
            (userinfo[..colon].to_owned(), userinfo[colon + 1..].to_owned())
        } else {
            (userinfo.to_owned(), String::new())
        };

        // Split off query string if present
        let (hostpath, query) = if let Some(q_pos) = hostpath.find('?') {
            (&hostpath[..q_pos], &hostpath[q_pos + 1..])
        } else {
            (hostpath, "")
        };

        // Split host:port from /database
        let (hostport, database) = if let Some(slash) = hostpath.find('/') {
            (&hostpath[..slash], hostpath[slash + 1..].to_owned())
        } else {
            (hostpath, String::new())
        };

        // Parse host:port
        let (host, port) = if let Some(colon) = hostport.rfind(':') {
            let port_str = &hostport[colon + 1..];
            let port: u16 = port_str
                .parse()
                .map_err(|_| ClientError::InvalidUrl(format!("invalid port: {port_str}")))?;
            (hostport[..colon].to_owned(), port)
        } else {
            (hostport.to_owned(), default_port)
        };

        if host.is_empty() {
            return Err(ClientError::InvalidUrl("host is required".into()));
        }

        // Parse query parameters. `dialect=` is the short alias the CLI
        // exposes; it is a synonym of the original `syntax_mode=` name
        // and either spelling sets the same field.
        let mut syntax_mode = None;
        for param in query.split('&') {
            if param.is_empty() { continue; }
            if let Some((key, value)) = param.split_once('=') {
                match key {
                    "syntax_mode" | "dialect" => syntax_mode = parse_syntax_mode(value),
                    _ => {}
                }
            }
        }

        Ok(Self {
            scheme,
            host,
            port,
            database,
            user,
            password,
            tls_skip_verify: false,
            unix_socket_path: None,
            syntax_mode,
        })
    }
}

/// Parse a syntax mode string.
fn parse_syntax_mode(s: &str) -> Option<SyntaxMode> {
    if s.eq_ignore_ascii_case("pyrosql") || s.eq_ignore_ascii_case("vsql") {
        Some(SyntaxMode::PyroSQL)
    } else if s.eq_ignore_ascii_case("postgresql")
        || s.eq_ignore_ascii_case("postgres")
        || s.eq_ignore_ascii_case("pg")
    {
        Some(SyntaxMode::PostgreSQL)
    } else if s.eq_ignore_ascii_case("mysql") {
        Some(SyntaxMode::MySQL)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_url() {
        let cfg = ConnectConfig::from_url("vsql://admin:secret@db.example.com:5000/mydb").unwrap();
        assert_eq!(cfg.host, "db.example.com");
        assert_eq!(cfg.port, 5000);
        assert_eq!(cfg.user, "admin");
        assert_eq!(cfg.password, "secret");
        assert_eq!(cfg.database, "mydb");
    }

    #[test]
    fn parse_minimal_url() {
        let cfg = ConnectConfig::from_url("vsql://localhost").unwrap();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 12520);
        assert!(cfg.user.is_empty());
        assert!(cfg.database.is_empty());
    }

    #[test]
    fn parse_url_default_port() {
        let cfg = ConnectConfig::from_url("vsql://user:pass@host/db").unwrap();
        assert_eq!(cfg.port, 12520);
        assert_eq!(cfg.user, "user");
        assert_eq!(cfg.password, "pass");
        assert_eq!(cfg.database, "db");
    }

    #[test]
    fn parse_url_no_user() {
        let cfg = ConnectConfig::from_url("vsql://myhost:9999/testdb").unwrap();
        assert_eq!(cfg.host, "myhost");
        assert_eq!(cfg.port, 9999);
        assert!(cfg.user.is_empty());
        assert_eq!(cfg.database, "testdb");
    }

    #[test]
    fn reject_unknown_scheme() {
        assert!(ConnectConfig::from_url("http://localhost").is_err());
    }

    #[test]
    fn parse_postgres_url() {
        let cfg = ConnectConfig::from_url("postgres://admin:pass@db.example.com/mydb").unwrap();
        assert_eq!(cfg.scheme, Scheme::Postgres);
        assert_eq!(cfg.host, "db.example.com");
        assert_eq!(cfg.port, 5432); // default PG port
        assert_eq!(cfg.user, "admin");
        assert_eq!(cfg.password, "pass");
        assert_eq!(cfg.database, "mydb");
    }

    #[test]
    fn parse_pg_url_alias() {
        let cfg = ConnectConfig::from_url("pg://localhost:5433/testdb").unwrap();
        assert_eq!(cfg.scheme, Scheme::Postgres);
        assert_eq!(cfg.port, 5433);
        assert_eq!(cfg.database, "testdb");
    }

    #[test]
    fn parse_mysql_url() {
        let cfg = ConnectConfig::from_url("mysql://root:secret@mysql-host:3307/appdb").unwrap();
        assert_eq!(cfg.scheme, Scheme::MySQL);
        assert_eq!(cfg.host, "mysql-host");
        assert_eq!(cfg.port, 3307);
        assert_eq!(cfg.user, "root");
        assert_eq!(cfg.database, "appdb");
    }

    #[test]
    fn parse_mysql_url_default_port() {
        let cfg = ConnectConfig::from_url("mysql://localhost/mydb").unwrap();
        assert_eq!(cfg.port, 3306);
    }

    #[test]
    fn parse_syntax_mode_query_param() {
        let cfg = ConnectConfig::from_url("vsql://localhost/mydb?syntax_mode=mysql").unwrap();
        assert_eq!(cfg.syntax_mode, Some(SyntaxMode::MySQL));
    }

    #[test]
    fn parse_syntax_mode_pg_alias() {
        let cfg = ConnectConfig::from_url("postgres://localhost/mydb?syntax_mode=vsql").unwrap();
        assert_eq!(cfg.syntax_mode, Some(SyntaxMode::PyroSQL));
    }

    #[test]
    fn dialect_query_param_is_alias_for_syntax_mode() {
        // `?dialect=…` mirrors `?syntax_mode=…` verbatim so the CLI's
        // short alias works through the URL parser too.
        let cfg = ConnectConfig::from_url("vsql://localhost/mydb?dialect=pg").unwrap();
        assert_eq!(cfg.syntax_mode, Some(SyntaxMode::PostgreSQL));
        let cfg = ConnectConfig::from_url("vsql://localhost/mydb?dialect=mysql").unwrap();
        assert_eq!(cfg.syntax_mode, Some(SyntaxMode::MySQL));
    }

    #[test]
    fn dialect_builder_is_alias_for_syntax_mode() {
        let cfg = ConnectConfig::new("localhost", 12520).dialect(SyntaxMode::MySQL);
        assert_eq!(cfg.syntax_mode, Some(SyntaxMode::MySQL));
    }

    #[test]
    fn unix_url_accepts_dialect_alias() {
        let cfg = ConnectConfig::from_url("unix:///tmp/x.sock?dialect=mysql").unwrap();
        assert_eq!(cfg.syntax_mode, Some(SyntaxMode::MySQL));
    }

    #[test]
    fn parse_unix_url() {
        let cfg = ConnectConfig::from_url("unix:///var/run/pyrosql/pyrosql.sock?database=mydb&user=admin").unwrap();
        assert_eq!(cfg.unix_socket_path.as_deref(), Some("/var/run/pyrosql/pyrosql.sock"));
        assert_eq!(cfg.database, "mydb");
        assert_eq!(cfg.user, "admin");
        assert!(cfg.tls_skip_verify); // Unix socket skips TLS
    }

    #[test]
    fn parse_unix_url_minimal() {
        let cfg = ConnectConfig::from_url("unix:///tmp/vsql.sock").unwrap();
        assert_eq!(cfg.unix_socket_path.as_deref(), Some("/tmp/vsql.sock"));
        assert!(cfg.database.is_empty());
        assert!(cfg.user.is_empty());
    }

    #[test]
    fn parse_auto_url() {
        let cfg = ConnectConfig::from_url("auto://localhost:12520/mydb").unwrap();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 12520);
        assert_eq!(cfg.database, "mydb");
        assert!(cfg.unix_socket_path.is_none());
    }

    #[test]
    fn quic_alias_still_works() {
        let cfg = ConnectConfig::from_url("quic://localhost:12520/mydb").unwrap();
        assert_eq!(cfg.scheme, Scheme::Quic);
        assert_eq!(cfg.port, 12520);
        assert_eq!(cfg.database, "mydb");
    }

    #[test]
    fn reject_empty_unix_path() {
        assert!(ConnectConfig::from_url("unix://").is_err());
    }

    #[test]
    fn builder_pattern() {
        let cfg = ConnectConfig::new("10.0.0.1", 12520)
            .database("prod")
            .user("root")
            .password("p4ss")
            .tls_skip_verify(true);
        assert_eq!(cfg.host, "10.0.0.1");
        assert_eq!(cfg.database, "prod");
        assert!(cfg.tls_skip_verify);
    }

    #[test]
    fn builder_unix_socket() {
        let cfg = ConnectConfig::new("localhost", 12520)
            .unix_socket("/var/run/pyrosql/pyrosql.sock")
            .database("testdb");
        assert_eq!(cfg.unix_socket_path.as_deref(), Some("/var/run/pyrosql/pyrosql.sock"));
        assert_eq!(cfg.database, "testdb");
    }
}
