//! One native connection: a TCP stream with reused request and reply
//! buffers, opened with a `hello` handshake.

use super::wire::{self, DocWriter, HEADER_LEN, protocol};
use ::mongodb::bson::raw::{Error as RawError, RawBsonRef as Raw, RawDocument};
use std::fmt;
use std::io::{Error, ErrorKind};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

type Res<T> = Result<T, Error>;
/// A connection and what its handshake reported.
type Opened = (Conn, Hello);
/// How far a command document may exceed `maxBsonObjectSize`.
const COMMAND_SLACK: usize = 16 * 1024;

/// The driver's minimum for MongoDB 5.0 (`maxWireVersion` 13), the oldest
/// server the backend supports.
pub(super) const MIN_WIRE_VERSION: i32 = 13;

/// A server address.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct Address {
    pub(super) host: String,
    pub(super) port: u16,
}

impl Address {
    /// Parse `host:port` (`[v6]:port`) as `hello` reports members; the port
    /// defaults to 27017.
    pub(super) fn parse(s: &str) -> Option<Self> {
        let (host, port) = if let Some(rest) = s.strip_prefix('[') {
            let (host, tail) = rest.split_once(']')?;
            (host, tail.strip_prefix(':'))
        } else {
            match s.rsplit_once(':') {
                Some((host, port)) => (host, Some(port)),
                None => (s, None),
            }
        };
        let port = match port {
            Some(p) => p.parse().ok()?,
            None => 27017,
        };
        (!host.is_empty()).then(|| Self {
            host: host.to_owned(),
            port,
        })
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

/// What a `hello` reply says about a server.
#[derive(Debug, Clone, Default)]
pub(super) struct Hello {
    pub(super) writable_primary: bool,
    pub(super) set_name: Option<String>,
    pub(super) max_wire_version: i32,
    pub(super) max_message_size: usize,
    pub(super) max_bson_size: usize,
    /// The primary this member knows of (`primary`).
    pub(super) primary: Option<String>,
    /// Members it lists (`hosts`).
    pub(super) hosts: Vec<String>,
}

impl Hello {
    fn parse(doc: &RawDocument) -> Res<Self> {
        let get = |key| doc.get(key).ok().flatten();
        let flag = |key| matches!(get(key), Some(Raw::Boolean(true)));
        let int = |key| match get(key) {
            Some(Raw::Int32(v)) => Some(i64::from(v)),
            Some(Raw::Int64(v)) => Some(v),
            Some(Raw::Double(v)) if v.fract() == 0.0 => Some(v as i64),
            _ => None,
        };
        let size = |key, default| {
            int(key)
                .and_then(|v| usize::try_from(v).ok())
                .unwrap_or(default)
        };
        let text = |key| doc.get_str(key).ok().map(str::to_owned);
        Ok(Self {
            writable_primary: flag("isWritablePrimary") || flag("ismaster"),
            set_name: text("setName"),
            max_wire_version: int("maxWireVersion")
                .and_then(|v| i32::try_from(v).ok())
                .unwrap_or_default(),
            max_message_size: size("maxMessageSizeBytes", 48_000_000),
            max_bson_size: size("maxBsonObjectSize", 16 * 1024 * 1024),
            primary: text("primary"),
            hosts: doc
                .get_array("hosts")
                .map(|hosts| hosts.into_iter().filter_map(host).collect())
                .unwrap_or_default(),
        })
    }

    /// Why this server cannot serve the native path as the primary of
    /// replica set `set_name` (`None` when it can).
    pub(super) fn unusable(&self, set_name: Option<&str>) -> Option<String> {
        if !self.writable_primary {
            return Some("not a writable primary".into());
        }
        if set_name.is_some() && self.set_name.as_deref() != set_name {
            return Some(format!(
                "replica set {:?}, expected {:?}",
                self.set_name.as_deref().unwrap_or_default(),
                set_name.unwrap_or_default()
            ));
        }
        if self.max_wire_version < MIN_WIRE_VERSION {
            return Some(format!(
                "maxWireVersion {} is older than MongoDB 5.0",
                self.max_wire_version
            ));
        }
        None
    }
}

/// A member address listed in a `hello` reply.
fn host(entry: Result<Raw<'_>, RawError>) -> Option<String> {
    entry.ok()?.as_str().map(str::to_owned)
}

/// Settings for opening connections.
#[derive(Debug, Clone)]
pub(super) struct ConnSpec {
    pub(super) connect_timeout: Duration,
    pub(super) app_name: Option<String>,
}

/// Where a connection is in its request/reply exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Phase {
    /// No exchange in progress: the connection can be reused.
    Idle,
    /// The request may be partly written: the connection must be closed.
    Writing,
    /// The request was sent and the reply is (partly) unread: reading the
    /// rest makes the connection reusable again.
    Reading,
}

/// One connection. The [`Phase`] records an exchange cut short (its future
/// dropped), so a connection is only reused at a message boundary.
pub(super) struct Conn {
    stream: TcpStream,
    out: Vec<u8>,
    reply: Vec<u8>,
    request_id: i32,
    phase: Phase,
    /// Pool generation the connection was opened in.
    pub(super) generation: u64,
    max_message_size: usize,
    max_bson_size: usize,
}

impl Conn {
    /// Connect to `addr` and run the `hello` handshake, within the connect
    /// timeout.
    pub(super) async fn open(addr: &Address, spec: &ConnSpec) -> Res<Opened> {
        let handshake = Self::handshake(addr, spec);
        let why = || format!("connect to {addr}");
        tokio::time::timeout(spec.connect_timeout, handshake)
            .await
            .map_err(|_| Error::new(ErrorKind::TimedOut, why()))?
    }

    async fn handshake(addr: &Address, spec: &ConnSpec) -> Res<Opened> {
        let host = (addr.host.as_str(), addr.port);
        let stream = TcpStream::connect(host).await?;
        stream.set_nodelay(true)?;
        let mut conn = Self {
            stream,
            out: Vec::with_capacity(512),
            reply: Vec::with_capacity(4096),
            request_id: 0,
            phase: Phase::Idle,
            generation: 0,
            max_message_size: 48_000_000,
            max_bson_size: 16 * 1024 * 1024,
        };
        let mut cmd = conn.begin();
        cmd.i32("hello", 1)?;
        cmd.bool("helloOk", true)?;
        let mut client = cmd.sub("client")?;
        if let Some(name) = &spec.app_name {
            let mut app = client.sub("application")?;
            app.str("name", name)?;
            app.finish()?;
        }
        let mut driver = client.sub("driver")?;
        driver.str("name", "siderite-native")?;
        driver.str("version", env!("CARGO_PKG_VERSION"))?;
        driver.finish()?;
        let mut os = client.sub("os")?;
        os.str("type", std::env::consts::OS)?;
        os.finish()?;
        client.finish()?;
        cmd.str("$db", "admin")?;
        cmd.finish()?;
        conn.round_trip().await?;
        let hello = Hello::parse(conn.reply()?)?;
        conn.max_message_size = hello.max_message_size;
        conn.max_bson_size = hello.max_bson_size;
        Ok((conn, hello))
    }

    /// Where the connection is in its exchange.
    pub(super) fn phase(&self) -> Phase {
        self.phase
    }

    /// Start a request; write the command document with the returned
    /// writer and [`finish`](DocWriter::finish) it, then call
    /// [`round_trip`](Self::round_trip).
    pub(super) fn begin(&mut self) -> DocWriter<'_> {
        wire::begin_request(&mut self.out);
        DocWriter::begin(&mut self.out)
    }

    /// Whether the request written since [`begin`](Self::begin) fits the
    /// server's limits (a command document may exceed `maxBsonObjectSize`
    /// by [`COMMAND_SLACK`]).
    pub(super) fn fits(&self) -> bool {
        let body = wire::request_body(&self.out).len();
        let limit = self.max_bson_size + COMMAND_SLACK;
        self.out.len() <= self.max_message_size && body <= limit
    }

    /// Send the request and read the whole reply.
    pub(super) async fn round_trip(&mut self) -> Res<()> {
        self.request_id = self.request_id.wrapping_add(1);
        wire::finish_request(&mut self.out, self.request_id)?;
        self.phase = Phase::Writing;
        self.stream.write_all(&self.out).await?;
        self.reply.clear();
        self.phase = Phase::Reading;
        self.finish_reply().await
    }

    /// Read (the rest of) the reply to the request in flight into `reply`,
    /// in as few reads as it arrives in. Cancel-safe: dropped, it leaves
    /// what it read in the buffer and can be called again.
    pub(super) async fn finish_reply(&mut self) -> Res<()> {
        loop {
            let want = if self.reply.len() < HEADER_LEN {
                HEADER_LEN
            } else {
                let len = wire::message_len(&self.reply)?;
                if len > self.max_message_size.max(48_000_000) {
                    let why = format!("MongoDB reply of {len} bytes");
                    return Err(protocol(why));
                }
                if self.reply.len() == len {
                    self.phase = Phase::Idle;
                    return Ok(());
                }
                if self.reply.len() > len {
                    // One request is in flight at a time, so nothing may
                    // follow its reply.
                    let why = "unexpected bytes after MongoDB reply";
                    return Err(protocol(why));
                }
                len
            };
            self.reply.reserve(want - self.reply.len());
            if self.stream.read_buf(&mut self.reply).await? == 0 {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "MongoDB closed the connection",
                ));
            }
        }
    }

    /// The body of the last reply.
    pub(super) fn reply(&self) -> Res<&RawDocument> {
        wire::reply_body(&self.reply, self.request_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::mongodb::bson::rawdoc;

    #[test]
    fn addresses_parse() {
        let a = Address::parse("db.example:27018").unwrap();
        assert_eq!((a.host.as_str(), a.port), ("db.example", 27018));
        assert_eq!(Address::parse("h").unwrap().port, 27017);
        let v6 = Address::parse("[::1]:27019").unwrap();
        assert_eq!((v6.host.as_str(), v6.port), ("::1", 27019));
        assert_eq!(v6.to_string(), "[::1]:27019");
        assert!(Address::parse(":1").is_none());
        assert!(Address::parse("h:x").is_none());
    }

    #[test]
    fn hello_checks() {
        let doc = rawdoc! {
            "isWritablePrimary": true, "setName": "rs0",
            "maxWireVersion": 25_i32,
            "maxMessageSizeBytes": 48_000_000_i32,
            "maxBsonObjectSize": 16_777_216_i32,
            "hosts": ["a:1", "b:2"], "primary": "a:1",
        };
        let hello = Hello::parse(&doc).unwrap();
        assert_eq!(hello.unusable(Some("rs0")), None);
        assert_eq!(hello.unusable(None), None);
        assert!(hello.unusable(Some("other")).is_some());
        assert_eq!(hello.hosts, ["a:1", "b:2"]);
        assert_eq!(hello.primary.as_deref(), Some("a:1"));
        let old = Hello {
            max_wire_version: 9,
            ..hello.clone()
        };
        assert!(old.unusable(None).is_some());
        let secondary = Hello {
            writable_primary: false,
            ..hello
        };
        assert!(secondary.unusable(Some("rs0")).is_some());
    }
}
