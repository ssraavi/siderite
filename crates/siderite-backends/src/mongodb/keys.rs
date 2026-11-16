//! Block allocation of generated integer keys (hi-lo, like a SQL sequence
//! with `CACHE n`).
//!
//! The durable state is one `{_id: <collection>, seq: <last reserved key>}`
//! document per collection in `siderite_counters`. Instead of one `$inc` per
//! insert, a backend reserves a block of keys with one `$inc` and hands them
//! out from memory until the block runs out. Clones of a backend and its
//! transactions share the blocks.
//!
//! Concurrency: the cached range of a collection sits behind a sync mutex
//! that is never held across an `await`, so taking keys from a block costs
//! no I/O and no async lock. When a block is exhausted, one task refills it
//! (`refill`, an async mutex per collection) while the others wait for that
//! refill and then re-check the range; keys come only from ranges that the
//! server's atomic `$inc` reserved for this process, and each key is handed
//! out at most once.

use ::mongodb::Database;
use ::mongodb::bson::{Bson, Document, doc};
use ::mongodb::options::ReturnDocument;
use siderite_orm::{BackendError, OrmError, QueryError};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

/// Collection holding one `{_id: <collection>, seq: <last key>}` document per
/// collection with generated integer keys.
pub(super) const COUNTERS: &str = "siderite_counters";

/// Default number of keys reserved per counter round trip.
pub(super) const DEFAULT_BLOCK_SIZE: u32 = 64;

/// Keys reserved but not yet handed out: `next..end` (end exclusive).
#[derive(Debug, Default, Clone, Copy)]
struct Range {
    next: i64,
    end: i64,
}

impl Range {
    /// Take `n` consecutive keys if the range still holds them.
    fn take(&mut self, n: i64) -> Option<i64> {
        let after = self.next.checked_add(n)?;
        if after > self.end {
            return None;
        }
        let first = self.next;
        self.next = after;
        Some(first)
    }
}

/// Per-collection state.
#[derive(Debug, Default)]
struct Slot {
    range: Mutex<Range>,
    /// Held by the one task that reserves a new block from the server.
    refill: tokio::sync::Mutex<()>,
}

impl Slot {
    fn take(&self, n: i64) -> Option<i64> {
        self.range
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take(n)
    }
}

/// Cached key blocks of one backend, shared by its clones and transactions.
#[derive(Debug)]
pub(super) struct KeyAllocator {
    block_size: i64,
    slots: Mutex<HashMap<String, Arc<Slot>>>,
}

impl Default for KeyAllocator {
    fn default() -> Self {
        Self::new(DEFAULT_BLOCK_SIZE)
    }
}

impl KeyAllocator {
    /// An allocator reserving `block_size` keys per round trip (at least 1).
    pub(super) fn new(block_size: u32) -> Self {
        Self {
            block_size: i64::from(block_size.max(1)),
            slots: Mutex::new(HashMap::new()),
        }
    }

    fn slot(&self, collection: &str) -> Arc<Slot> {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(slot) = slots.get(collection) {
            return Arc::clone(slot);
        }
        let slot = Arc::new(Slot::default());
        slots.insert(collection.to_owned(), Arc::clone(&slot));
        slot
    }

    /// Reserve `n` consecutive integer keys for `collection`; returns the
    /// first. Served from the cached block when it holds `n` more keys;
    /// otherwise one `$inc` of `max(n, block_size)` on the counter document
    /// reserves a new block (the rest of the old one is dropped, leaving a
    /// gap). The counter update runs outside any transaction, like a
    /// sequence.
    pub(super) async fn reserve(
        &self,
        db: &Database,
        collection: &str,
        n: usize,
    ) -> Result<i64, OrmError> {
        let n = i64::try_from(n).map_err(|_| QueryError::InvalidPlan("too many rows".into()))?;
        let slot = self.slot(collection);
        if let Some(first) = slot.take(n) {
            return Ok(first);
        }
        let _refilling = slot.refill.lock().await;
        // Another task may have refilled while this one waited.
        if let Some(first) = slot.take(n) {
            return Ok(first);
        }
        let size = n.max(self.block_size);
        let last = increment(db, collection, size).await?;
        // `last` is the end of the new block: keys `last - size + 1 ..= last`.
        let first = last - size + 1;
        let next = first + n;
        *slot.range.lock().unwrap_or_else(PoisonError::into_inner) = Range {
            next,
            end: last + 1,
        };
        Ok(first)
    }
}

/// `$inc` the counter of `collection` by `by` (creating it at `by`); returns
/// the new `seq`.
async fn increment(db: &Database, collection: &str, by: i64) -> Result<i64, OrmError> {
    let counters = db.collection::<Document>(COUNTERS);
    let after = counters
        .find_one_and_update(doc! { "_id": collection }, doc! { "$inc": { "seq": by } })
        .upsert(true)
        .return_document(ReturnDocument::After)
        .await
        .map_err(|e| OrmError::from(super::exec::map_error(&e)))?
        .and_then(|d| d.get("seq").and_then(as_i64))
        .ok_or_else(|| BackendError::Database("key counter has no `seq`".into()))?;
    Ok(after)
}

fn as_i64(value: &Bson) -> Option<i64> {
    match value {
        Bson::Int32(i) => Some(i64::from(*i)),
        Bson::Int64(i) => Some(*i),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::Range;

    #[test]
    fn range_hands_out_consecutive_keys_until_exhausted() {
        let mut r = Range { next: 5, end: 9 };
        assert_eq!(r.take(2), Some(5));
        assert_eq!(r.take(1), Some(7));
        assert_eq!(r.take(2), None);
        assert_eq!(r.take(1), Some(8));
        assert_eq!(r.take(1), None);
        assert_eq!(Range::default().take(1), None);
        let mut full = Range {
            next: i64::MAX - 1,
            end: i64::MAX,
        };
        assert_eq!(full.take(2), None);
        assert_eq!(full.take(1), Some(i64::MAX - 1));
    }
}
