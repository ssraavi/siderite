//! Native reads: `find` / `aggregate`, then `getMore` on the same
//! connection until the cursor is exhausted, `killCursors` when the read
//! stops early.

use super::NativeError;
use super::conn::Conn;
use super::pool::{Lease, Pool};
use super::wire::{DocWriter, Res};
use crate::mongodb::compile::{FindQuery, Read};
use ::mongodb::bson::Document;
use ::mongodb::bson::raw::{RawArray, RawBsonRef, RawDocument};
use ::mongodb::error::CommandError;
use siderite_orm::OrmError;
use std::sync::Arc;
use std::time::Duration;

/// The outcome of a native exchange.
type Native<T> = Result<T, NativeError>;
/// The cursor id after a batch, or why delivery stopped.
type Delivered = Result<i64, Stop>;

/// Server error codes after which the connection's server is no longer a
/// usable primary (or the connection is broken): the pool looks for the
/// primary again and the read is retried once through the driver.
const STATE_CHANGE_CODES: [i32; 11] = [
    10107, // NotWritablePrimary
    13435, // NotPrimaryNoSecondaryOk
    13436, // NotPrimaryOrSecondary
    189,   // PrimarySteppedDown
    91,    // ShutdownInProgress
    11600, // InterruptedAtShutdown
    11602, // InterruptedDueToReplStateChange
    7,     // HostNotFound
    6,     // HostUnreachable
    89,    // NetworkTimeout
    9001,  // SocketException
];

/// How long a cancelled request's reply is waited for before its
/// connection is closed instead.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// What a read runs, and where.
pub(super) struct Target<'a> {
    pub(super) db: &'a str,
    pub(super) collection: &'a str,
    pub(super) read_concern: Option<&'a Document>,
}

/// Run `read` on the pool's primary, handing each document to `each`.
/// `delivered` turns true once `each` has seen a document, after which the
/// read must not be retried.
pub(super) async fn run<F>(
    pool: &Arc<Pool>,
    target: &Target<'_>,
    read: &Read,
    each: &mut F,
    delivered: &mut bool,
) -> Native<()>
where
    F: FnMut(&RawDocument) -> Result<(), OrmError>,
{
    let mut lease = pool.checkout().await?;
    let batch_size = match read {
        Read::Find(find) => {
            write_find(conn(&mut lease)?, target, find)?;
            find.batch_size()
        }
        Read::Pipeline(pipeline) => {
            write_pipeline(conn(&mut lease)?, target, pipeline)?;
            None
        }
    };
    exchange(&mut lease).await?;
    let mut id = deliver(conn(&mut lease)?, "firstBatch", each, delivered);
    // A cursor left open is killed on drop (the request future may be
    // dropped at any await below).
    let mut open: Option<OpenCursor> = None;
    loop {
        let cursor_id = match id {
            Ok(0) => return Ok(()),
            Ok(cursor_id) => cursor_id,
            Err(Stop::Reply(e)) => return Err(e),
            Err(Stop::Sink(e, cursor_id)) => {
                if cursor_id != 0 {
                    kill(&mut lease, target, cursor_id).await;
                }
                if let Some(open) = &mut open {
                    open.id = 0;
                }
                return Err(NativeError::Orm(e));
            }
        };
        let guard = open.get_or_insert_with(|| OpenCursor::new(pool, target));
        guard.id = cursor_id;
        write_get_more(conn(&mut lease)?, target, cursor_id, batch_size)?;
        exchange(&mut lease).await?;
        id = deliver(conn(&mut lease)?, "nextBatch", each, delivered);
    }
}

fn conn<'l>(lease: &'l mut Lease<'_>) -> Result<&'l mut Conn, NativeError> {
    lease
        .conn()
        .ok_or_else(|| NativeError::Unavailable("connection retired".into()))
}

/// Why delivering a batch stopped.
enum Stop {
    /// The reply was an error or malformed.
    Reply(NativeError),
    /// `each` failed; the cursor (id) may still be open.
    Sink(OrmError, i64),
}

/// Hand the documents of the reply's `field` batch to `each` and return
/// the cursor id.
fn deliver<F>(c: &Conn, key: &str, each: &mut F, seen: &mut bool) -> Delivered
where
    F: FnMut(&RawDocument) -> Result<(), OrmError>,
{
    let bad = |e: &dyn std::fmt::Display| {
        Stop::Reply(NativeError::Network(super::wire::protocol(format!(
            "malformed cursor reply: {e}"
        ))))
    };
    let reply = c.reply().map_err(|e| Stop::Reply(e.into()))?;
    let (id, batch) = cursor_batch(reply, key).map_err(|e| bad(&e))?;
    for doc in batch {
        let doc = doc.map_err(|e| bad(&e))?;
        let RawBsonRef::Document(doc) = doc else {
            return Err(bad(&"batch element is not a document"));
        };
        *seen = true;
        each(doc).map_err(|e| Stop::Sink(e, id))?;
    }
    Ok(id)
}

fn cursor_batch<'r>(
    reply: &'r RawDocument,
    field: &str,
) -> Result<(i64, &'r RawArray), ::mongodb::bson::raw::ValueAccessError> {
    let cursor = reply.get_document("cursor")?;
    Ok((cursor.get_i64("id")?, cursor.get_array(field)?))
}

/// Send the request written into the lease's connection and check the
/// reply's `ok`. Network failures and "not primary" errors retire the
/// connection and send the pool looking for the primary.
pub(super) async fn exchange(lease: &mut Lease<'_>) -> Native<()> {
    let conn = conn(lease)?;
    if !conn.fits() {
        return Err(NativeError::Unavailable(
            "command exceeds the server's size limits".into(),
        ));
    }
    if let Err(e) = conn.round_trip().await {
        lease.retire();
        return Err(NativeError::Network(e));
    }
    let status = match conn.reply() {
        Ok(reply) => check_ok(reply),
        Err(e) => Err(NativeError::Network(e)),
    };
    if matches!(
        status,
        Err(NativeError::Network(_) | NativeError::Command(_, true))
    ) {
        lease.retire();
    }
    status
}

/// `Ok` for `ok: 1`, the server's error otherwise.
fn check_ok(reply: &RawDocument) -> Native<()> {
    let ok = match reply.get("ok") {
        Ok(Some(RawBsonRef::Double(v))) => v == 1.0,
        Ok(Some(RawBsonRef::Int32(v))) => v == 1,
        Ok(Some(RawBsonRef::Int64(v))) => v == 1,
        Ok(Some(RawBsonRef::Boolean(v))) => v,
        _ => false,
    };
    if ok {
        return Ok(());
    }
    match ::mongodb::bson::from_slice::<CommandError>(reply.as_bytes()) {
        Ok(e) => {
            let state_change = STATE_CHANGE_CODES.contains(&e.code);
            Err(NativeError::Command(e, state_change))
        }
        Err(_) => {
            let why = format!("command failed: {reply:?}");
            Err(NativeError::Server(why))
        }
    }
}

/// Kill cursor `id` on the lease's connection, ignoring the outcome (an
/// unkilled cursor times out on the server).
async fn kill(lease: &mut Lease<'_>, target: &Target<'_>, id: i64) {
    if let Ok(conn) = conn(lease)
        && write_kill(conn, target.db, target.collection, id).is_ok()
    {
        let _ = exchange(lease).await;
    }
}

/// Finish the exchange a cancelled request left on `conn`: read the reply,
/// kill the cursor it opened (if any), and return the connection to
/// `pool`. A connection that fails or takes too long is closed.
pub(super) async fn drain(pool: &Pool, mut conn: Conn) {
    let generation = conn.generation;
    if !matches!(
        tokio::time::timeout(DRAIN_TIMEOUT, conn.finish_reply()).await,
        Ok(Ok(()))
    ) {
        return;
    }
    let open = match conn.reply() {
        Ok(reply) => match check_ok(reply) {
            Ok(()) => left_open(reply),
            Err(NativeError::Command(_, true)) => {
                pool.retire(generation);
                return;
            }
            Err(_) => None,
        },
        Err(_) => return,
    };
    if let Some((db, collection, id)) = open {
        if write_kill(&mut conn, &db, &collection, id).is_err() {
            return;
        }
        let kill = conn.round_trip();
        let killed = tokio::time::timeout(DRAIN_TIMEOUT, kill).await;
        if !matches!(killed, Ok(Ok(()))) {
            return;
        }
    }
    pool.give_back(conn);
}

/// The database, collection and id of the cursor a reply leaves open.
fn left_open(reply: &RawDocument) -> Option<(String, String, i64)> {
    let cursor = reply.get_document("cursor").ok()?;
    let id = cursor.get_i64("id").ok().filter(|id| *id != 0)?;
    let (db, collection) = cursor.get_str("ns").ok()?.split_once('.')?;
    Some((db.to_owned(), collection.to_owned(), id))
}

/// A server cursor that is killed (from a spawned task) if the read stops
/// while it is open.
struct OpenCursor {
    pool: Arc<Pool>,
    db: String,
    collection: String,
    id: i64,
}

impl OpenCursor {
    fn new(pool: &Arc<Pool>, target: &Target<'_>) -> Self {
        Self {
            pool: Arc::clone(pool),
            db: target.db.to_owned(),
            collection: target.collection.to_owned(),
            id: 0,
        }
    }
}

impl Drop for OpenCursor {
    fn drop(&mut self) {
        if self.id == 0 {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let (pool, db, collection, id) = (
            Arc::clone(&self.pool),
            std::mem::take(&mut self.db),
            std::mem::take(&mut self.collection),
            self.id,
        );
        runtime.spawn(async move {
            if let Ok(mut lease) = pool.checkout().await {
                let target = Target {
                    db: &db,
                    collection: &collection,
                    read_concern: None,
                };
                kill(&mut lease, &target, id).await;
            }
        });
    }
}

fn write_find(conn: &mut Conn, at: &Target<'_>, find: &FindQuery) -> Res<()> {
    let mut cmd = conn.begin();
    cmd.str("find", at.collection)?;
    cmd.doc("filter", &find.filter)?;
    if let Some(sort) = &find.sort {
        cmd.doc("sort", sort)?;
    }
    if let Some(projection) = &find.projection {
        cmd.doc("projection", projection)?;
    }
    if let Some(skip) = find.skip {
        cmd.i64("skip", i64::try_from(skip).unwrap_or(i64::MAX))?;
    }
    if let Some(limit) = find.limit {
        cmd.i64("limit", limit)?;
    }
    if let Some(size) = find.batch_size() {
        // As the driver does: one more than the limit, so the server
        // closes the cursor with the first batch.
        let size = if Some(i64::from(size)) == find.limit {
            size + 1
        } else {
            size
        };
        cmd.i32("batchSize", i32::try_from(size).unwrap_or(i32::MAX))?;
    }
    finish_read(cmd, at)
}

fn write_pipeline(c: &mut Conn, at: &Target<'_>, p: &[Document]) -> Res<()> {
    let mut cmd = c.begin();
    cmd.str("aggregate", at.collection)?;
    let mut array = cmd.array("pipeline")?;
    for stage in p {
        array.push_doc(stage)?;
    }
    array.finish()?;
    cmd.sub("cursor")?.finish()?;
    finish_read(cmd, at)
}

fn finish_read(mut cmd: DocWriter<'_>, at: &Target<'_>) -> Res<()> {
    if let Some(read_concern) = at.read_concern {
        cmd.doc("readConcern", read_concern)?;
    }
    cmd.str("$db", at.db)?;
    cmd.finish()
}

fn write_get_more(
    conn: &mut Conn,
    target: &Target<'_>,
    id: i64,
    batch_size: Option<u32>,
) -> Res<()> {
    let mut cmd = conn.begin();
    cmd.i64("getMore", id)?;
    cmd.str("collection", target.collection)?;
    if let Some(size) = batch_size {
        cmd.i32("batchSize", i32::try_from(size).unwrap_or(i32::MAX))?;
    }
    cmd.str("$db", target.db)?;
    cmd.finish()
}

fn write_kill(conn: &mut Conn, db: &str, coll: &str, id: i64) -> Res<()> {
    let mut cmd = conn.begin();
    cmd.str("killCursors", coll)?;
    let mut ids = cmd.array("cursors")?;
    ids.push_i64(id);
    ids.finish()?;
    cmd.str("$db", db)?;
    cmd.finish()
}
