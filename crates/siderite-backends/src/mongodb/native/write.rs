//! Native writes: `insert`, `update`, `delete` and the key counter's
//! `findAndModify`, with the database's write concern and, for the
//! commands the retryable-writes rules allow, one retry with the same
//! session and transaction number.
//!
//! What is retryable follows the driver: `insert` (every batch) and
//! `findAndModify` when the URL does not say `retryWrites=false` and the
//! server supports sessions (a replica set member or `mongos`); `update`
//! with `multi: true` and `delete` with `limit: 0` never are, and carry no
//! session. A retryable command carries `lsid` and `txnNumber`. When it
//! fails with a network error after it may have been sent, or with an
//! error the server labels `RetryableWriteError` (or one of the
//! not-primary, shutdown and network codes), its connection generation is
//! retired, the primary is found again (waiting for an election up to the
//! server selection timeout) and the same command is sent once more with
//! the same `lsid` and `txnNumber`, so the server applies it at most once.
//! A write never moves to the driver once it may have been sent (the
//! driver could not reuse the session); only a write the native pool
//! cannot start ([`NativeError::Unavailable`]) is left to the driver.

use super::conn::Conn;
use super::cursor::{self, STATE_CHANGE_CODES, check_ok};
use super::pool::Lease;
use super::session::{SessionLease, Txn};
use super::wire::{DocWriter, Res, protocol};
use super::{Native, NativeError};
use crate::mongodb::compile::UpdateSpec;
use ::mongodb::bson::raw::{RawBsonRef, RawDocument};
use ::mongodb::bson::spec::BinarySubtype;
use ::mongodb::bson::{Document, from_slice};
use ::mongodb::error::{ErrorKind, InsertManyError, WriteConcernError};
use ::mongodb::error::{WriteError, WriteFailure};
use std::time::Instant;

/// The outcome of a native write step.
type Step<T> = Result<T, NativeError>;

/// `ExceededTimeLimit`, retryable for writes (not a state change).
const EXCEEDED_TIME_LIMIT: i32 = 262;
/// The label of errors a retryable write may be retried after.
const RETRYABLE_WRITE_ERROR: &str = "RetryableWriteError";
/// The label of a retry's error that means the retry wrote nothing, so the
/// first attempt's error is the one to report.
const NO_WRITES_PERFORMED: &str = "NoWritesPerformed";

/// Whether a server error code allows retrying a retryable write.
fn retryable_code(code: i32) -> bool {
    STATE_CHANGE_CODES.contains(&code) || code == EXCEEDED_TIME_LIMIT
}

/// Whether `doc` (a reply or its `writeConcernError`) carries `label`.
fn has_label(doc: &RawDocument, label: &str) -> bool {
    doc.get_array("errorLabels").is_ok_and(|labels| {
        labels
            .into_iter()
            .any(|l| l.ok().and_then(|l| l.as_str()) == Some(label))
    })
}

/// A failed attempt.
struct Failed {
    error: NativeError,
    /// Documents the attempt's command held.
    count: usize,
    /// Whether the command may be sent again.
    retry: bool,
    /// The server says this attempt wrote nothing.
    no_writes: bool,
}

impl Failed {
    fn fatal(error: NativeError) -> Box<Self> {
        Box::new(Self {
            error,
            count: 0,
            retry: false,
            no_writes: false,
        })
    }
}

/// One write (possibly several commands, as insert batches), on one
/// connection and session.
struct Writer<'n> {
    native: &'n Native,
    lease: Option<Lease<'n>>,
    session: Option<SessionLease<'n>>,
    /// The writes retry (and carry a session).
    retryable: bool,
    /// A command may have reached the server: the driver must not take
    /// over.
    sent: bool,
}

impl<'n> Writer<'n> {
    /// Check out a connection (and, for a retryable command kind, a
    /// session). [`NativeError::Unavailable`] when the pool has no primary
    /// right now.
    async fn open(native: &'n Native, retryable: bool) -> Step<Self> {
        let mut lease = native.pool.checkout().await?;
        let timeout = lease.conn().and_then(|c| c.retryable_writes());
        let retry = retryable && native.writes.retry_writes;
        let session = timeout
            .filter(|_| retry)
            .map(|timeout| native.sessions.checkout(timeout));
        Ok(Self {
            native,
            lease: Some(lease),
            retryable: session.is_some(),
            session,
            sent: false,
        })
    }

    /// Run one command: `write` puts it into the connection (with the
    /// session fields, if any) and returns how many documents it holds;
    /// `read` turns the `ok: 1` reply into the result.
    async fn command<T, W, R>(&mut self, write: &mut W, read: &mut R) -> Step<(T, usize)>
    where
        W: FnMut(&mut Conn, Option<Txn>) -> Res<usize>,
        R: FnMut(&RawDocument) -> Step<T>,
    {
        let txn = self.session.as_mut().and_then(SessionLease::next_txn);
        let first = match self.attempt(txn, None, write, read).await {
            Ok(done) => return Ok(done),
            Err(failed) if !failed.retry => return Err(self.settle(failed.error)),
            Err(failed) => failed,
        };
        // Retry once, natively, on a connection to the (new) primary.
        self.lease = None;
        let wait = self.native.writes.selection_timeout;
        let deadline = Instant::now() + wait;
        let Ok(mut lease) = self.native.pool.checkout_by(deadline).await else {
            return Err(self.settle(first.error));
        };
        if lease.conn().and_then(|c| c.retryable_writes()).is_none() {
            return Err(self.settle(first.error));
        }
        self.lease = Some(lease);
        let expect = Some(first.count);
        match self.attempt(txn, expect, write, read).await {
            Ok(done) => Ok(done),
            Err(again) if again.no_writes => Err(self.settle(first.error)),
            Err(again) => Err(self.settle(again.error)),
        }
    }

    /// Once a command may have been sent, a failure is final: it must not
    /// read as [`NativeError::Unavailable`] (which hands the write to the
    /// driver).
    fn settle(&self, error: NativeError) -> NativeError {
        match error {
            NativeError::Unavailable(why) if self.sent => NativeError::Lost(why),
            other => other,
        }
    }

    /// Send the command once. `expect` is the document count of the
    /// attempt being retried, which the retry must repeat exactly.
    async fn attempt<T, W, R>(
        &mut self,
        txn: Option<Txn>,
        expect: Option<usize>,
        write: &mut W,
        read: &mut R,
    ) -> Result<(T, usize), Box<Failed>>
    where
        W: FnMut(&mut Conn, Option<Txn>) -> Res<usize>,
        R: FnMut(&RawDocument) -> Step<T>,
    {
        let gone = || NativeError::Unavailable("connection retired".into());
        let lease = self.lease.as_mut().ok_or_else(|| Failed::fatal(gone()))?;
        let conn = lease.conn().ok_or_else(|| Failed::fatal(gone()))?;
        let unsent = |e: std::io::Error| NativeError::Unavailable(e.to_string());
        let count = write(conn, txn).map_err(|e| Failed::fatal(unsent(e)))?;
        if expect.is_some_and(|n| n != count) {
            let why = "the server's size limits changed before a retry";
            return Err(Failed::fatal(NativeError::Unavailable(why.into())));
        }
        if !conn.fits() {
            let why = "command exceeds the server's size limits";
            return Err(Failed::fatal(NativeError::Unavailable(why.into())));
        }
        self.sent = true;
        if let Err(error) = cursor::send(lease).await {
            if let Some(session) = &mut self.session {
                session.mark_dirty();
            }
            return Err(Box::new(Failed {
                error,
                count,
                retry: self.retryable,
                no_writes: false,
            }));
        }
        let conn = lease.conn().ok_or_else(|| Failed::fatal(gone()))?;
        let reply = match conn.reply() {
            Ok(reply) => reply,
            Err(e) => {
                lease.retire();
                if let Some(session) = &mut self.session {
                    session.mark_dirty();
                }
                return Err(Failed::fatal(NativeError::Network(e)));
            }
        };
        let no_writes = has_label(reply, NO_WRITES_PERFORMED);
        let labelled = has_label(reply, RETRYABLE_WRITE_ERROR);
        let (outcome, retry, state_change) = match check_ok(reply) {
            Err(NativeError::Command(e, state_change)) => {
                let retry = self.retryable && (labelled || retryable_code(e.code));
                (
                    Err(NativeError::Command(e, state_change)),
                    retry,
                    state_change,
                )
            }
            Err(other) => (Err(other), false, false),
            Ok(()) => {
                let retry = self.retryable && retryable_concern(reply, labelled);
                (read(reply), retry, false)
            }
        };
        if retry || state_change {
            lease.retire();
        }
        match outcome {
            Ok(done) if !retry => Ok((done, count)),
            Ok(_) => Err(Box::new(Failed {
                error: NativeError::Server("retryable write concern error".into()),
                count,
                retry,
                no_writes,
            })),
            Err(error) => Err(Box::new(Failed {
                error,
                count,
                retry,
                no_writes,
            })),
        }
    }

    /// Return the session for reuse (after success or a server error; a
    /// network error has marked it dirty, and a writer dropped without
    /// `finish`, a cancelled write, discards it).
    fn finish(mut self) {
        if let Some(session) = self.session.take() {
            session.release();
        }
    }
}

/// Whether an `ok: 1` reply failed its write concern in a way that allows
/// a retry.
fn retryable_concern(reply: &RawDocument, labelled: bool) -> bool {
    let Ok(wce) = reply.get_document("writeConcernError") else {
        return false;
    };
    labelled
        || has_label(wce, RETRYABLE_WRITE_ERROR)
        || wce.get_i32("code").is_ok_and(retryable_code)
}

impl Native {
    /// Insert `docs` into `collection` in order, in as many commands as the
    /// server's batch and message limits need; returns how many were
    /// inserted.
    pub(in crate::mongodb) async fn insert(
        &self,
        collection: &str,
        docs: &[Document],
    ) -> Step<u64> {
        let mut writer = Writer::open(self, true).await?;
        let limit = self.writes.max_write_batch;
        let (mut at, mut inserted) = (0, 0);
        while at < docs.len() {
            let rest = &docs[at..];
            let mut write = |conn: &mut Conn, txn| {
                let max = conn.max_write_batch_size().min(limit.unwrap_or(usize::MAX));
                self.write_insert(conn, collection, rest, max.max(1), txn)
            };
            let mut read = |reply: &RawDocument| insert_reply(reply, at);
            match writer.command(&mut write, &mut read).await {
                Ok((n, count)) => {
                    inserted += n;
                    at += count;
                }
                Err(e) => {
                    writer.finish();
                    return Err(e);
                }
            }
        }
        writer.finish();
        Ok(inserted)
    }

    /// Update the documents matching `filter`; returns how many matched.
    pub(in crate::mongodb) async fn update(
        &self,
        collection: &str,
        filter: &Document,
        update: &UpdateSpec,
    ) -> Step<u64> {
        let mut writer = Writer::open(self, false).await?;
        let mut write = |conn: &mut Conn, txn| {
            let mut cmd = conn.begin();
            cmd.str("update", collection)?;
            let mut updates = cmd.array("updates")?;
            let mut statement = updates.push_sub()?;
            statement.doc("q", filter)?;
            match update {
                UpdateSpec::Set(doc) => statement.doc("u", doc)?,
                UpdateSpec::Pipeline(stages) => {
                    let mut u = statement.array("u")?;
                    for stage in stages {
                        u.push_doc(stage)?;
                    }
                    u.finish()?;
                }
            }
            statement.bool("multi", true)?;
            statement.finish()?;
            updates.finish()?;
            cmd.bool("ordered", true)?;
            self.finish_write(cmd, txn).map(|()| 0)
        };
        let done = writer.command(&mut write, &mut single_write_n).await;
        writer.finish();
        done.map(|(n, _)| n)
    }

    /// Delete the documents matching `filter`; returns how many were
    /// deleted.
    pub(in crate::mongodb) async fn delete(
        &self,
        collection: &str,
        filter: &Document,
    ) -> Step<u64> {
        let mut writer = Writer::open(self, false).await?;
        let mut write = |conn: &mut Conn, txn| {
            let mut cmd = conn.begin();
            cmd.str("delete", collection)?;
            let mut deletes = cmd.array("deletes")?;
            let mut statement = deletes.push_sub()?;
            statement.doc("q", filter)?;
            statement.i32("limit", 0)?;
            statement.finish()?;
            deletes.finish()?;
            cmd.bool("ordered", true)?;
            self.finish_write(cmd, txn).map(|()| 0)
        };
        let done = writer.command(&mut write, &mut single_write_n).await;
        writer.finish();
        done.map(|(n, _)| n)
    }

    /// `$inc` the `seq` of document `id` in `counters` by `by`, creating
    /// it, and return the new value (the key counter's `findAndModify`).
    pub(in crate::mongodb) async fn increment(
        &self,
        counters: &str,
        id: &str,
        by: i64,
    ) -> Step<i64> {
        let mut writer = Writer::open(self, true).await?;
        let mut write = |conn: &mut Conn, txn| {
            let mut cmd = conn.begin();
            cmd.str("findAndModify", counters)?;
            let mut query = cmd.sub("query")?;
            query.str("_id", id)?;
            query.finish()?;
            let mut update = cmd.sub("update")?;
            let mut inc = update.sub("$inc")?;
            inc.i64("seq", by)?;
            inc.finish()?;
            update.finish()?;
            cmd.bool("new", true)?;
            cmd.bool("upsert", true)?;
            self.finish_write(cmd, txn).map(|()| 0)
        };
        let mut read = |reply: &RawDocument| {
            single_write_error(reply)?;
            let seq = reply
                .get_document("value")
                .ok()
                .and_then(|v| match v.get("seq") {
                    Ok(Some(RawBsonRef::Int64(n))) => Some(n),
                    Ok(Some(RawBsonRef::Int32(n))) => Some(i64::from(n)),
                    _ => None,
                });
            seq.ok_or_else(|| NativeError::Server("key counter has no `seq`".into()))
        };
        let done = writer.command(&mut write, &mut read).await;
        writer.finish();
        done.map(|(seq, _)| seq)
    }

    /// Write an `insert` of the leading documents of `docs` that fit (at
    /// most `max`); returns how many it holds.
    fn write_insert(
        &self,
        conn: &mut Conn,
        collection: &str,
        docs: &[Document],
        max: usize,
        txn: Option<Txn>,
    ) -> Res<usize> {
        let mut cmd = conn.begin();
        cmd.str("insert", collection)?;
        cmd.bool("ordered", true)?;
        self.finish_write(cmd, txn)?;
        let mut sequence = conn.sequence("documents")?;
        for doc in docs {
            if !sequence.push(doc, max)? {
                break;
            }
        }
        let count = sequence.finish()?;
        if count == 0 {
            return Err(protocol("document exceeds the server's size limits"));
        }
        Ok(count)
    }

    /// Close a write command: the write concern, the session fields of a
    /// retryable write, `$db`.
    fn finish_write(&self, mut cmd: DocWriter<'_>, txn: Option<Txn>) -> Res<()> {
        if let Some(concern) = &self.writes.write_concern {
            cmd.doc("writeConcern", concern)?;
        }
        if let Some(txn) = txn {
            let mut lsid = cmd.sub("lsid")?;
            lsid.binary("id", BinarySubtype::Uuid.into(), &txn.id)?;
            lsid.finish()?;
            cmd.i64("txnNumber", txn.number)?;
        }
        cmd.str("$db", &self.db)?;
        cmd.finish()
    }
}

/// The `n` of a reply.
fn reply_n(reply: &RawDocument) -> Step<u64> {
    let n = match reply.get("n") {
        Ok(Some(RawBsonRef::Int32(n))) => u64::try_from(n).ok(),
        Ok(Some(RawBsonRef::Int64(n))) => u64::try_from(n).ok(),
        Ok(Some(RawBsonRef::Double(n))) if n >= 0.0 && n.fract() == 0.0 => Some(n as u64),
        _ => None,
    };
    n.ok_or_else(|| NativeError::Server(format!("write reply without `n`: {reply:?}")))
}

/// The outcome of an `insert` batch whose first document is document `at`
/// of the whole insert: `n`, or the batch's write errors (indexes counted
/// from the first document of the insert, as the driver reports them) and
/// write concern error, as the driver's `InsertMany` error.
fn insert_reply(reply: &RawDocument, at: usize) -> Step<u64> {
    let failed = reply.get("writeErrors").ok().flatten().is_some()
        || reply.get("writeConcernError").ok().flatten().is_some();
    if failed {
        let mut error: InsertManyError = from_slice(reply.as_bytes()).map_err(malformed)?;
        for e in error.write_errors.iter_mut().flatten() {
            e.index += at;
        }
        return Err(NativeError::Write(Box::new(ErrorKind::InsertMany(error))));
    }
    reply_n(reply)
}

/// The `n` of an `update` / `delete` reply, after [`single_write_error`].
fn single_write_n(reply: &RawDocument) -> Step<u64> {
    single_write_error(reply)?;
    reply_n(reply)
}

/// The first write error, else the write concern error, of a
/// single-statement reply, as the driver reports them.
fn single_write_error(reply: &RawDocument) -> Step<()> {
    let failure = if let Ok(errors) = reply.get_array("writeErrors")
        && let Some(first) = errors.into_iter().next()
    {
        let first = first.map_err(malformed)?;
        let RawBsonRef::Document(first) = first else {
            return Err(NativeError::Server("malformed write error".into()));
        };
        let error: WriteError = from_slice(first.as_bytes()).map_err(malformed)?;
        WriteFailure::WriteError(error)
    } else if let Ok(concern) = reply.get_document("writeConcernError") {
        let error: WriteConcernError = from_slice(concern.as_bytes()).map_err(malformed)?;
        WriteFailure::WriteConcernError(error)
    } else {
        return Ok(());
    };
    Err(NativeError::Write(Box::new(ErrorKind::Write(failure))))
}

fn malformed(e: impl std::fmt::Display) -> NativeError {
    NativeError::Server(format!("malformed write reply: {e}"))
}
