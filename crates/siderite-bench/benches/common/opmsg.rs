//! A minimal MongoDB `OP_MSG` client on one persistent TCP stream: the
//! floor the driver and ORM variants are measured against.
//!
//! It sends one kind-0 section per request and reads the reply's 16-byte
//! header and body into reused buffers. It does no handshake, sessions,
//! cluster-time gossip, retries, compression or authentication, so it
//! needs a server that accepts unauthenticated commands. Validation is
//! light: the reply must answer this request with a single kind-0 document,
//! `ok` must be 1, and the caller's check must pass.

use mongodb::bson::raw::RawBsonRef;
use mongodb::bson::{Document, RawDocument};
use std::error::Error;
use std::sync::atomic::{AtomicI32, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

/// Errors from the wire client: I/O, BSON and protocol failures.
pub type WireError = Box<dyn Error + Send + Sync>;

/// Result of a wire operation.
pub type WireResult<T> = Result<T, WireError>;

/// `OP_MSG` opcode.
const OP_MSG: i32 = 2013;
/// Bytes in a message header (length, request id, response to, opcode).
const HEADER_LEN: usize = 16;
/// Bytes before the first section document: flag bits and the section kind.
const PREFIX_LEN: usize = 5;

/// The stream and the buffers reused for every request.
struct Io {
    stream: TcpStream,
    out: Vec<u8>,
    reply: Vec<u8>,
}

/// One connection that runs commands one at a time.
pub struct Conn {
    io: Mutex<Io>,
    next_id: AtomicI32,
}

/// The little-endian `i32` at `at` in `bytes`.
fn le_i32(bytes: &[u8], at: usize) -> WireResult<i32> {
    let field = bytes.get(at..at + 4).ok_or("message too short")?;
    Ok(i32::from_le_bytes(field.try_into()?))
}

/// Fail unless `reply` has `ok: 1`, quoting the server's message if not.
fn check_ok(reply: &RawDocument) -> WireResult<()> {
    let ok = match reply.get("ok")? {
        Some(RawBsonRef::Double(v)) => v >= 1.0,
        Some(RawBsonRef::Int32(v)) => v == 1,
        Some(RawBsonRef::Int64(v)) => v == 1,
        _ => false,
    };
    if ok {
        return Ok(());
    }
    let message = match reply.get("errmsg")? {
        Some(RawBsonRef::String(text)) => text.to_owned(),
        _ => "command failed".to_owned(),
    };
    Err(message.into())
}

impl Conn {
    /// Connect to `addr` (`host:port`) with Nagle's algorithm off, as the
    /// driver does.
    ///
    /// # Errors
    /// When the server cannot be reached.
    pub async fn connect(addr: &str) -> WireResult<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            io: Mutex::new(Io {
                stream,
                out: Vec::with_capacity(512),
                reply: Vec::with_capacity(512),
            }),
            next_id: AtomicI32::new(1),
        })
    }

    /// Run `command` (which must name its database in `$db`), check the
    /// reply is `ok` and pass it to `check`.
    ///
    /// # Errors
    /// I/O and BSON errors, a malformed or mismatched reply, a command
    /// error, or whatever `check` rejects.
    pub async fn run(
        &self,
        command: &Document,
        check: impl FnOnce(&RawDocument) -> WireResult<()>,
    ) -> WireResult<()> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut guard = self.io.lock().await;
        let Io { stream, out, reply } = &mut *guard;

        out.clear();
        out.resize(HEADER_LEN, 0);
        // Flag bits 0, then section kind 0 (one body document).
        out.extend_from_slice(&[0; PREFIX_LEN]);
        command.to_writer(&mut *out)?;
        let len = i32::try_from(out.len())?;
        out[0..4].copy_from_slice(&len.to_le_bytes());
        out[4..8].copy_from_slice(&id.to_le_bytes());
        out[12..16].copy_from_slice(&OP_MSG.to_le_bytes());
        stream.write_all(out).await?;

        let mut header = [0; HEADER_LEN];
        stream.read_exact(&mut header).await?;
        if le_i32(&header, 8)? != id || le_i32(&header, 12)? != OP_MSG {
            return Err("reply does not answer this OP_MSG request".into());
        }
        let body_len = usize::try_from(le_i32(&header, 0)?)?
            .checked_sub(HEADER_LEN)
            .ok_or("reply length below header size")?;
        reply.resize(body_len, 0);
        stream.read_exact(reply).await?;

        if reply.get(4) != Some(&0) {
            return Err("reply does not start with a kind-0 section".into());
        }
        let doc_len = usize::try_from(le_i32(reply, PREFIX_LEN)?)?;
        let body = reply
            .get(PREFIX_LEN..PREFIX_LEN + doc_len)
            .ok_or("reply document overruns the message")?;
        let doc = RawDocument::from_bytes(body)?;
        check_ok(doc)?;
        check(doc)
    }
}
