//! Server sessions for native retryable writes.
//!
//! A retryable write carries `lsid` (a session id the client makes up: a
//! random UUID, BSON binary subtype 4) and a `txnNumber` that grows with
//! every write on that session; the server remembers the outcome of the
//! last write per session, so a retry with the same pair is applied at
//! most once. As in the driver's session pool, a session serves one
//! command at a time, the most recently used one is reused first, one idle
//! for longer than the server's `logicalSessionTimeoutMinutes` less a
//! minute is discarded (the server may have forgotten it), and so is one
//! whose command hit a network error. A [`SessionLease`] dropped without
//! [`release`](SessionLease::release) (its write future was cancelled)
//! discards its session too, so its last `txnNumber` is never followed by
//! another write while the cancelled one may still run on the server.
//! Discarded sessions are not ended explicitly; the server expires them.

use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

/// How long before the server's session timeout a session is discarded.
const EXPIRY_MARGIN: Duration = Duration::from_secs(60);

/// The session fields of one retryable write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Txn {
    /// `lsid.id`.
    pub(super) id: [u8; 16],
    /// `txnNumber`.
    pub(super) number: i64,
}

#[derive(Debug)]
struct ServerSession {
    id: [u8; 16],
    /// The last `txnNumber` used.
    txn_number: i64,
    last_use: Instant,
}

impl ServerSession {
    fn new() -> Self {
        Self {
            id: uuid::Uuid::new_v4().into_bytes(),
            txn_number: 0,
            last_use: Instant::now(),
        }
    }

    /// Whether the server may forget the session within a minute.
    fn expiring(&self, timeout: Duration) -> bool {
        self.last_use.elapsed() >= timeout.saturating_sub(EXPIRY_MARGIN)
    }
}

/// Idle server sessions, most recently used last.
#[derive(Debug, Default)]
pub(super) struct SessionPool {
    idle: Mutex<Vec<ServerSession>>,
}

impl SessionPool {
    /// A session for writes to a server whose session timeout is
    /// `timeout`: the most recently used idle one that is not about to
    /// expire, else a new one.
    pub(super) fn checkout(&self, timeout: Duration) -> SessionLease<'_> {
        let reused = {
            let mut idle = self.lock();
            loop {
                match idle.pop() {
                    Some(s) if s.expiring(timeout) => {}
                    other => break other,
                }
            }
        };
        SessionLease {
            pool: self,
            session: Some(reused.unwrap_or_else(ServerSession::new)),
            timeout,
            dirty: false,
        }
    }

    /// Number of idle sessions.
    #[cfg(test)]
    fn idle(&self) -> usize {
        self.lock().len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<ServerSession>> {
        self.idle.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A session checked out for one write (all its batches and its retry).
#[derive(Debug)]
pub(super) struct SessionLease<'p> {
    pool: &'p SessionPool,
    session: Option<ServerSession>,
    timeout: Duration,
    dirty: bool,
}

impl SessionLease<'_> {
    /// The session fields for the next command: a new `txnNumber`.
    pub(super) fn next_txn(&mut self) -> Option<Txn> {
        let session = self.session.as_mut()?;
        session.txn_number += 1;
        session.last_use = Instant::now();
        Some(Txn {
            id: session.id,
            number: session.txn_number,
        })
    }

    /// A command on this session hit a network error: discard it after
    /// use.
    pub(super) fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Return the session to the pool unless it is dirty or about to
    /// expire; sessions that expired while idle are dropped on the way.
    pub(super) fn release(mut self) {
        let Some(session) = self.session.take() else {
            return;
        };
        let mut idle = self.pool.lock();
        // The oldest sessions sit at the bottom of the stack.
        let fresh = idle
            .iter()
            .position(|s| !s.expiring(self.timeout))
            .unwrap_or(idle.len());
        idle.drain(..fresh);
        if !self.dirty && !session.expiring(self.timeout) {
            idle.push(session);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(30 * 60);

    #[test]
    fn sessions_are_reused_with_growing_txn_numbers() {
        let pool = SessionPool::default();
        let mut a = pool.checkout(TIMEOUT);
        let first = a.next_txn().unwrap();
        let second = a.next_txn().unwrap();
        assert_eq!((first.number, second.number), (1, 2));
        assert_eq!(first.id, second.id);
        // A second concurrent write gets another session.
        let mut b = pool.checkout(TIMEOUT);
        assert_ne!(b.next_txn().unwrap().id, first.id);
        b.release();
        a.release();
        assert_eq!(pool.idle(), 2);
        // The most recently returned session comes back first.
        let mut again = pool.checkout(TIMEOUT);
        let next = again.next_txn().unwrap();
        assert_eq!((next.id, next.number), (first.id, 3));
        again.release();
    }

    #[test]
    fn dirty_dropped_and_expiring_sessions_are_discarded() {
        let pool = SessionPool::default();
        let mut dirty = pool.checkout(TIMEOUT);
        let id = dirty.next_txn().unwrap().id;
        dirty.mark_dirty();
        dirty.release();
        assert_eq!(pool.idle(), 0);
        {
            // Dropped without release (a cancelled write).
            let mut dropped = pool.checkout(TIMEOUT);
            assert_ne!(dropped.next_txn().unwrap().id, id);
        }
        assert_eq!(pool.idle(), 0);
        // With a timeout under a minute every session counts as expiring.
        let short = Duration::from_secs(30);
        pool.checkout(TIMEOUT).release();
        assert_eq!(pool.idle(), 1);
        let mut fresh = pool.checkout(short);
        assert_eq!(fresh.next_txn().unwrap().number, 1);
        assert_eq!(pool.idle(), 0);
        fresh.release();
        assert_eq!(pool.idle(), 0);
    }
}
