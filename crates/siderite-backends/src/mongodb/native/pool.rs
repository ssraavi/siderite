//! The native connection pool: a fixed number of permits, idle connections
//! on a stack, and the primary they connect to.
//!
//! Checkout takes a semaphore permit (an atomic in the uncontended case)
//! and pops an idle connection under a short `std` mutex, so a request
//! never hops to another task. A connection is opened lazily when none is
//! idle. A [`Lease`] puts its connection back on drop when its last
//! exchange completed and the pool has not moved on to a new generation. A
//! lease dropped while a reply is still due (its request future was
//! cancelled) hands the connection, and its permit, to a task that reads
//! the reply to the end, kills the cursor it may have opened, and only
//! then returns the connection; one dropped mid-write, or after a network
//! error or a "not primary" reply, is closed.
//!
//! The primary is found with `hello` on the seed list (following the
//! `primary` and `hosts` each member reports). An error that means the
//! primary is gone bumps the generation, which retires every connection
//! opened before it and forgets the primary; the next checkout runs the
//! discovery again. While discovery runs, or for a short while after it
//! failed, checkout reports [`Checkout::Unavailable`] and the caller uses
//! the driver.

use super::conn::{Address, Conn, ConnSpec, Hello, Phase};
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, SemaphorePermit};

/// How long a failed discovery keeps the native path unavailable before
/// the next attempt.
const DISCOVERY_BACKOFF: Duration = Duration::from_millis(250);

/// Why a checkout did not produce a connection.
#[derive(Debug)]
pub(super) enum Checkout {
    /// No primary is known right now (or it could not be reached); the
    /// driver should serve the request.
    Unavailable(String),
    /// Every connection stayed busy for the whole wait.
    Timeout(Duration),
}

/// A checkout's outcome.
type Checked<'a> = Result<Lease<'a>, Checkout>;

fn unavailable(why: impl Into<String>) -> Checkout {
    Checkout::Unavailable(why.into())
}

/// Settings of a pool.
#[derive(Debug, Clone)]
pub(super) struct PoolSpec {
    pub(super) seeds: Vec<Address>,
    /// Whether discovery may follow the members a seed reports (not with
    /// `directConnection=true`).
    pub(super) follow_members: bool,
    pub(super) set_name: Option<String>,
    pub(super) max_connections: usize,
    pub(super) wait_timeout: Duration,
    pub(super) connect: ConnSpec,
}

#[derive(Debug, Default)]
struct Primary {
    address: Option<Address>,
    failed_at: Option<Instant>,
}

/// A fixed-size pool of connections to the primary.
pub(super) struct Pool {
    spec: PoolSpec,
    permits: Semaphore,
    idle: Mutex<Vec<Conn>>,
    generation: AtomicU64,
    primary: Mutex<Primary>,
    discovering: tokio::sync::Mutex<()>,
}

fn locked<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Pool {
    /// Find the primary and keep its first connection.
    ///
    /// # Errors
    /// Why no member of the seed list is a usable primary.
    pub(super) async fn connect(spec: PoolSpec) -> Result<Self, String> {
        let pool = Self {
            permits: Semaphore::new(spec.max_connections.max(1)),
            spec,
            idle: Mutex::new(Vec::new()),
            generation: AtomicU64::new(0),
            primary: Mutex::new(Primary::default()),
            discovering: tokio::sync::Mutex::new(()),
        };
        let conn = pool.discover().await.map_err(|e| match e {
            Checkout::Unavailable(why) => why,
            Checkout::Timeout(_) => "timed out".into(),
        })?;
        locked(&pool.idle).push(conn);
        Ok(pool)
    }

    /// The pool's size.
    pub(super) fn max_connections(&self) -> usize {
        self.spec.max_connections.max(1)
    }

    /// The primary connections go to, if one is known.
    pub(super) fn primary(&self) -> Option<Address> {
        locked(&self.primary).address.clone()
    }

    /// Wait (bounded) for a permit and hand out a connection to the
    /// primary.
    pub(super) async fn checkout(self: &Arc<Self>) -> Checked<'_> {
        let permit = self.permit().await?;
        let generation = self.generation.load(Ordering::Acquire);
        let reused = {
            let mut idle = locked(&self.idle);
            loop {
                match idle.pop() {
                    Some(c) if c.generation == generation => break Some(c),
                    Some(_) => {}
                    None => break None,
                }
            }
        };
        let conn = match reused {
            Some(conn) => conn,
            None => self.open(generation).await?,
        };
        Ok(Lease {
            pool: self,
            conn: Some(conn),
            permit: Some(permit),
        })
    }

    /// A permit, waiting at most the wait timeout for one.
    async fn permit(&self) -> Result<SemaphorePermit<'_>, Checkout> {
        if let Ok(permit) = self.permits.try_acquire() {
            return Ok(permit);
        }
        let wait = self.spec.wait_timeout;
        let acquire = self.permits.acquire();
        match tokio::time::timeout(wait, acquire).await {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) => Err(unavailable("pool closed")),
            Err(_) => Err(Checkout::Timeout(wait)),
        }
    }

    /// Why `hello` rules its server out as the pool's primary.
    fn unusable(&self, hello: &Hello) -> Option<String> {
        hello.unusable(self.spec.set_name.as_deref())
    }

    /// A new connection to the known primary, or to one found now.
    async fn open(&self, generation: u64) -> Result<Conn, Checkout> {
        let Some(address) = self.primary() else {
            return self.discover().await;
        };
        match Conn::open(&address, &self.spec.connect).await {
            Ok((mut conn, hello)) => match self.unusable(&hello) {
                None => {
                    conn.generation = generation;
                    Ok(conn)
                }
                Some(why) => {
                    self.retire(generation);
                    Err(Checkout::Unavailable(format!("{address}: {why}")))
                }
            },
            Err(e) => {
                self.retire(generation);
                Err(Checkout::Unavailable(format!("{address}: {e}")))
            }
        }
    }

    /// Look for the primary on the seed list and the members they report,
    /// and return a connection to it.
    async fn discover(&self) -> Result<Conn, Checkout> {
        let Ok(_running) = self.discovering.try_lock() else {
            return Err(Checkout::Unavailable(
                "primary discovery in progress".into(),
            ));
        };
        let generation = self.generation.load(Ordering::Acquire);
        let (known, failed_recently) = {
            let primary = locked(&self.primary);
            let failed = primary
                .failed_at
                .is_some_and(|at| at.elapsed() < DISCOVERY_BACKOFF);
            (primary.address.is_some(), failed)
        };
        if known {
            // Found by the discovery that just finished.
            return Box::pin(self.open(generation)).await;
        }
        if failed_recently {
            return Err(unavailable("no primary found recently"));
        }
        let mut queue: VecDeque<_> = self.spec.seeds.iter().cloned().collect();
        let mut seen = HashSet::new();
        let mut last = String::from("empty seed list");
        while let Some(address) = queue.pop_front() {
            if !seen.insert(address.clone()) {
                continue;
            }
            match Conn::open(&address, &self.spec.connect).await {
                Ok((mut conn, hello)) => match self.unusable(&hello) {
                    None => {
                        conn.generation = generation;
                        *locked(&self.primary) = Primary {
                            address: Some(address),
                            failed_at: None,
                        };
                        return Ok(conn);
                    }
                    Some(why) => {
                        last = format!("{address}: {why}");
                        if self.spec.follow_members {
                            queue.extend(reported_members(&hello));
                        }
                    }
                },
                Err(e) => last = format!("{address}: {e}"),
            }
        }
        locked(&self.primary).failed_at = Some(Instant::now());
        Err(Checkout::Unavailable(format!("no primary: {last}")))
    }

    /// Retire every connection of `generation` (if it is still current)
    /// and forget the primary, so the next checkout looks for it again.
    pub(super) fn retire(&self, generation: u64) {
        if self
            .generation
            .compare_exchange(
                generation,
                generation + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            locked(&self.primary).address = None;
            let stale = std::mem::take(&mut *locked(&self.idle));
            drop(stale);
        }
    }

    /// Keep `conn` for reuse if it sits at a message boundary and belongs
    /// to the current generation; close it otherwise.
    pub(super) fn give_back(&self, conn: Conn) {
        let current = self.generation.load(Ordering::Acquire);
        if conn.phase() == Phase::Idle && conn.generation == current {
            locked(&self.idle).push(conn);
        }
    }
}

/// The primary first, then the other members a `hello` reports.
fn reported_members(hello: &Hello) -> impl Iterator<Item = Address> + '_ {
    hello
        .primary
        .iter()
        .chain(&hello.hosts)
        .filter_map(|s| Address::parse(s))
}

/// A checked-out connection; dropping it returns the connection (see the
/// module docs).
pub(super) struct Lease<'a> {
    pool: &'a Arc<Pool>,
    conn: Option<Conn>,
    permit: Option<SemaphorePermit<'a>>,
}

impl Lease<'_> {
    /// The connection. `None` only after [`retire`](Self::retire).
    pub(super) fn conn(&mut self) -> Option<&mut Conn> {
        self.conn.as_mut()
    }

    /// Close the connection and make the pool look for the primary again.
    pub(super) fn retire(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.retire(conn.generation);
        }
    }
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        let Some(conn) = self.conn.take() else {
            return;
        };
        match conn.phase() {
            Phase::Idle => self.pool.give_back(conn),
            Phase::Writing => {}
            Phase::Reading => {
                let Ok(runtime) = tokio::runtime::Handle::try_current() else {
                    return;
                };
                // The permit travels with the connection: it is still busy.
                if let Some(permit) = self.permit.take() {
                    permit.forget();
                }
                let pool = Arc::clone(self.pool);
                runtime.spawn(async move {
                    super::cursor::drain(&pool, conn).await;
                    pool.permits.add_permits(1);
                });
            }
        }
    }
}
