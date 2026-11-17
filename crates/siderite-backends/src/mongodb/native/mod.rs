//! The native data path (feature `mongodb-native`): reads sent as `OP_MSG`
//! messages on a small connection pool of the backend's own, bypassing the
//! driver's per-operation server selection, implicit session and pool
//! hand-off. See the "Native data path" section of the
//! [module docs](crate::mongodb) for what it covers.

mod conn;
mod cursor;
mod pool;
mod wire;

use super::compile::Read;
use super::exec::map_error;
use ::mongodb::bson::raw::RawDocument;
use ::mongodb::bson::{Document, to_document};
use ::mongodb::error::{CommandError, Error as DriverError, ErrorKind};
use ::mongodb::options::SelectionCriteria as Selection;
use ::mongodb::options::{ClientOptions, ReadPreference, ServerAddress, Tls};
use conn::{Address, ConnSpec};
use pool::{Checkout, Pool, PoolSpec};
use siderite_orm::{BackendError, OrmError};
use std::sync::Arc;
use std::time::Duration;

/// The driver's default `maxPoolSize`.
const DEFAULT_MAX_CONNECTIONS: usize = 10;
/// The driver's default `connectTimeoutMS`.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Wait for a free native connection when the URL sets no
/// `waitQueueTimeoutMS` (the driver's default server selection timeout).
const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// Which path a [`MongoBackend`](super::MongoBackend) sends its reads on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DataPath {
    /// Everything goes through the `mongodb` driver.
    Driver,
    /// Reads outside transactions use the native `OP_MSG` pool; everything
    /// else uses the driver.
    Native,
}

/// Settings of the native data path; unset fields come from the URL.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct NativeOptions {
    /// Connections in the native pool (default: the URL's `maxPoolSize`,
    /// else 10 like the driver). They are opened on demand, in addition to
    /// the driver's own pool.
    pub max_connections: Option<usize>,
    /// How long a read waits for a free connection before failing with
    /// [`BackendError::Connection`] (default: the URL's
    /// `waitQueueTimeoutMS`, else 30 s).
    pub wait_timeout: Option<Duration>,
}

impl NativeOptions {
    /// Set [`max_connections`](Self::max_connections).
    #[must_use]
    pub fn max_connections(mut self, n: usize) -> Self {
        self.max_connections = Some(n);
        self
    }

    /// Set [`wait_timeout`](Self::wait_timeout).
    #[must_use]
    pub fn wait_timeout(mut self, wait: Duration) -> Self {
        self.wait_timeout = Some(wait);
        self
    }
}

/// Whether a backend uses the native path.
#[derive(Clone, Default)]
pub(super) enum NativeState {
    /// Not requested.
    #[default]
    Off,
    /// In use.
    Active(Arc<Native>),
    /// Requested, but the deployment or URL does not qualify.
    Ineligible(Arc<str>),
}

impl std::fmt::Debug for NativeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Off => f.write_str("Off"),
            Self::Active(native) => f
                .debug_struct("Active")
                .field("primary", &native.pool.primary().map(primary))
                .field("max_connections", &native.pool.max_connections())
                .finish(),
            Self::Ineligible(why) => write!(f, "Ineligible({why:?})"),
        }
    }
}

impl NativeState {
    /// Decide once, at connect, whether `url` can use the native path for
    /// database `db`, and open its pool if so.
    pub(super) async fn connect(
        url: &str,
        db: &::mongodb::Database,
        options: &NativeOptions,
    ) -> Self {
        match Native::connect(url, db, options).await {
            Ok(native) => Self::Active(Arc::new(native)),
            Err(why) => Self::Ineligible(why.into()),
        }
    }

    pub(super) fn data_path(&self) -> DataPath {
        match self {
            Self::Active(_) => DataPath::Native,
            Self::Off | Self::Ineligible(_) => DataPath::Driver,
        }
    }

    pub(super) fn ineligibility(&self) -> Option<&str> {
        match self {
            Self::Ineligible(why) => Some(why),
            Self::Off | Self::Active(_) => None,
        }
    }

    pub(super) fn active(&self) -> Option<&Native> {
        match self {
            Self::Active(native) => Some(native),
            Self::Off | Self::Ineligible(_) => None,
        }
    }
}

/// An open native path: the pool and what every command carries.
pub(super) struct Native {
    pool: Arc<Pool>,
    db: String,
    read_concern: Option<Document>,
}

impl Native {
    async fn connect(
        url: &str,
        db: &::mongodb::Database,
        options: &NativeOptions,
    ) -> Result<Self, String> {
        if !url.starts_with("mongodb://") {
            return Err("only mongodb:// URLs (not SRV) are supported".into());
        }
        let parsed = ClientOptions::parse(url)
            .await
            .map_err(|e| format!("invalid URL: {e}"))?;
        let spec = pool_spec(&parsed, url, options)?;
        if !primary_reads(db.selection_criteria()) {
            return Err("the read preference is not primary".into());
        }
        let read_concern = db
            .read_concern()
            .map(to_document)
            .transpose()
            .map_err(|e| e.to_string())?;
        let pool = Pool::connect(spec).await?;
        Ok(Self {
            pool: Arc::new(pool),
            db: db.name().to_owned(),
            read_concern,
        })
    }

    /// Run `read` on `collection`, handing each document to `each`;
    /// `delivered` turns true once `each` has seen a document.
    pub(super) async fn read<F>(
        &self,
        collection: &str,
        read: &Read,
        each: &mut F,
        delivered: &mut bool,
    ) -> Result<(), NativeError>
    where
        F: FnMut(&RawDocument) -> Result<(), OrmError>,
    {
        let target = cursor::Target {
            db: &self.db,
            collection,
            read_concern: self.read_concern.as_ref(),
        };
        cursor::run(&self.pool, &target, read, each, delivered).await
    }
}

/// Check that the URL qualifies and build the pool settings.
fn pool_spec(
    parsed: &ClientOptions,
    url: &str,
    options: &NativeOptions,
) -> Result<PoolSpec, String> {
    if matches!(parsed.tls, Some(Tls::Enabled(_))) {
        return Err("TLS is not supported".into());
    }
    if parsed.credential.is_some() {
        return Err("authentication is not supported".into());
    }
    if parsed.load_balanced == Some(true) {
        return Err("load-balanced deployments are not supported".into());
    }
    if !primary_reads(parsed.selection_criteria.as_ref()) {
        return Err("the read preference is not primary".into());
    }
    if parsed.server_api.is_some() {
        return Err("a declared server API version is not supported".into());
    }
    let seeds = parsed
        .hosts
        .iter()
        .map(|host| match host {
            ServerAddress::Tcp { host, port } => Ok(Address {
                host: host.clone(),
                port: port.unwrap_or(27017),
            }),
            _ => Err("only TCP hosts are supported".to_owned()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PoolSpec {
        seeds,
        follow_members: parsed.direct_connection != Some(true),
        set_name: parsed.repl_set_name.clone(),
        max_connections: options
            .max_connections
            .or_else(|| parsed.max_pool_size.and_then(as_usize))
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_MAX_CONNECTIONS),
        wait_timeout: options
            .wait_timeout
            .or_else(|| url_millis(url, "waitQueueTimeoutMS"))
            .filter(|d| !d.is_zero())
            .unwrap_or(DEFAULT_WAIT_TIMEOUT),
        connect: ConnSpec {
            connect_timeout: parsed
                .connect_timeout
                .filter(|d| !d.is_zero())
                .unwrap_or(DEFAULT_CONNECT_TIMEOUT),
            app_name: parsed.app_name.clone(),
        },
    })
}

fn as_usize(n: u32) -> Option<usize> {
    usize::try_from(n).ok()
}

fn primary(address: conn::Address) -> String {
    address.to_string()
}

/// Whether `criteria` read from the primary only.
fn primary_reads(criteria: Option<&Selection>) -> bool {
    matches!(
        criteria,
        None | Some(Selection::ReadPreference(ReadPreference::Primary))
    )
}

/// A millisecond option the driver parses but does not expose.
fn url_millis(url: &str, key: &str) -> Option<Duration> {
    let (_, query) = url.split_once('?')?;
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        k.eq_ignore_ascii_case(key)
            .then(|| v.parse().ok().map(Duration::from_millis))
            .flatten()
    })
}

/// A native read that did not complete.
#[derive(Debug)]
pub(super) enum NativeError {
    /// The native path cannot serve the read now (no primary known, or
    /// the command exceeds the size limits); the driver should.
    Unavailable(String),
    /// No connection became free in time.
    Timeout(Duration),
    /// The connection failed (and was closed).
    Network(std::io::Error),
    /// The server answered `ok: 0`; `true` when the error means the
    /// server is no longer a usable primary.
    Command(CommandError, bool),
    /// An `ok: 0` reply without an error code.
    Server(String),
    /// The row sink failed.
    Orm(OrmError),
}

impl From<Checkout> for NativeError {
    fn from(e: Checkout) -> Self {
        match e {
            Checkout::Unavailable(why) => Self::Unavailable(why),
            Checkout::Timeout(wait) => Self::Timeout(wait),
        }
    }
}

impl From<std::io::Error> for NativeError {
    fn from(e: std::io::Error) -> Self {
        Self::Network(e)
    }
}

impl NativeError {
    /// Whether the driver should run the read instead (only valid while no
    /// document was delivered).
    pub(super) fn use_driver(&self) -> bool {
        matches!(
            self,
            Self::Unavailable(_) | Self::Network(_) | Self::Command(_, true)
        )
    }

    /// The error a caller sees, classified as the driver path classifies
    /// the same failure ([`map_error`]).
    pub(super) fn into_orm(self) -> OrmError {
        match self {
            Self::Unavailable(why) => {
                let why = format!("native MongoDB path unavailable: {why}");
                BackendError::Connection(why).into()
            }
            Self::Timeout(wait) => BackendError::Connection(format!(
                "no native MongoDB connection became free within {wait:?}"
            ))
            .into(),
            Self::Network(e) => map_error(&DriverError::from(e)).into(),
            Self::Command(e, _) => {
                let e = DriverError::from(ErrorKind::Command(e));
                map_error(&e).into()
            }
            Self::Server(message) => BackendError::Database(message).into(),
            Self::Orm(e) => e,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn spec(url: &str) -> Result<PoolSpec, String> {
        let parsed = ClientOptions::parse(url).await.unwrap();
        pool_spec(&parsed, url, &NativeOptions::default())
    }

    #[tokio::test]
    async fn eligibility_follows_the_url() {
        let url = concat!(
            "mongodb://a:1,b/?replicaSet=rs0&maxPoolSize=7",
            "&connectTimeoutMS=500&waitQueueTimeoutMS=250",
        );
        let ok = spec(url).await.unwrap();
        assert_eq!(ok.seeds.len(), 2);
        assert_eq!(ok.seeds[1].port, 27017);
        assert_eq!(ok.set_name.as_deref(), Some("rs0"));
        assert_eq!(ok.max_connections, 7);
        assert_eq!(ok.connect.connect_timeout, Duration::from_millis(500));
        assert_eq!(ok.wait_timeout, Duration::from_millis(250));
        assert!(ok.follow_members);
        let direct = spec("mongodb://a/?directConnection=true").await.unwrap();
        assert!(!direct.follow_members);
        let defaults = spec("mongodb://a").await.unwrap();
        assert_eq!(defaults.max_connections, DEFAULT_MAX_CONNECTIONS);
        assert_eq!(defaults.wait_timeout, DEFAULT_WAIT_TIMEOUT);
        assert_eq!(defaults.connect.connect_timeout, DEFAULT_CONNECT_TIMEOUT);
        for url in [
            "mongodb://u:p@a/",
            "mongodb://a/?tls=true",
            "mongodb://a/?loadBalanced=true",
            "mongodb://a/?readPreference=secondaryPreferred",
            "mongodb://a/?readPreference=nearest",
        ] {
            assert!(spec(url).await.is_err(), "{url}");
        }
    }

    #[test]
    fn url_millis_is_case_insensitive() {
        let url = "mongodb://a/?x=1&waitqueuetimeoutms=40";
        assert_eq!(
            url_millis(url, "waitQueueTimeoutMS"),
            Some(Duration::from_millis(40))
        );
        assert_eq!(url_millis("mongodb://a", "waitQueueTimeoutMS"), None);
    }

    #[test]
    fn errors_classify_like_the_driver_path() {
        use ::mongodb::bson::{doc, from_document};
        let duplicate: CommandError = from_document(doc! {
            "code": 11000, "codeName": "DuplicateKey", "errmsg": "E11000",
        })
        .unwrap();
        assert!(matches!(
            NativeError::Command(duplicate, false).into_orm(),
            OrmError::Backend(BackendError::Constraint(_))
        ));
        let reset = std::io::ErrorKind::ConnectionReset;
        let io = std::io::Error::new(reset, "reset");
        assert!(NativeError::Network(io).use_driver());
        assert!(matches!(
            NativeError::Timeout(Duration::from_millis(1)).into_orm(),
            OrmError::Backend(BackendError::Connection(_))
        ));
        assert!(!NativeError::Timeout(Duration::from_millis(1)).use_driver());
    }
}
