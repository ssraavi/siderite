//! Failover under read load: `replSetStepDown` on the primary of a
//! three-member replica set while concurrent reads run, once on the driver
//! path and once on the native path.
//!
//! Runs only when `MONGODB_FAILOVER_URL` names such a set (it steps the
//! primary down, so never point it at a shared deployment), for example
//! `mongodb://127.0.0.1:27031,127.0.0.1:27032,127.0.0.1:27033/`
//! `?replicaSet=rs7`; without it the test returns after a note. The
//! native path must fail no more reads than the driver path and must end
//! up on the new primary.

use mongodb::bson::{Document, doc};
use siderite_backends::mongodb::{DataPath, MongoBackend};
use siderite_orm::{Executor, Expr, QueryPlan, Value};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// A backend the reader tasks share.
type Shared = Arc<MongoBackend>;

const READERS: usize = 16;
const ITEMS: i64 = 100;

/// What one load run saw.
struct Outcome {
    ok: usize,
    failed: usize,
    /// From the step-down to the last failed read.
    last_failure: Option<Duration>,
    /// Longest stretch after the step-down without a successful read.
    longest_gap: Duration,
}

impl std::fmt::Debug for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ok, {} failed, last failure {:?} after the step-down, \
             longest gap {:?}",
            self.ok, self.failed, self.last_failure, self.longest_gap
        )
    }
}

fn item_doc(i: i64) -> Document {
    doc! { "_id": i, "n": i }
}

fn item(id: i64) -> QueryPlan {
    QueryPlan::from_table("items").filter(Expr::col("id").eq(Value::Int(id)))
}

/// The current primary's address, once the set has one.
async fn primary(backend: &MongoBackend) -> String {
    let admin = backend.database().client().database("admin");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(hello) = admin.run_command(doc! { "hello": 1 }).await
            && hello.get_bool("isWritablePrimary").unwrap_or(false)
            && let Ok(primary) = hello.get_str("primary")
        {
            return primary.to_owned();
        }
        assert!(Instant::now() < deadline, "no primary elected");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Read under load for `before`, step the primary down, keep reading for
/// `after`.
async fn run(db: Shared, before: Duration, after: Duration) -> Outcome {
    let backend = db;
    let stop = Arc::new(AtomicBool::new(false));
    let start = Instant::now();
    let readers: Vec<_> = (0..READERS)
        .map(|reader| {
            let (backend, stop) = (Arc::clone(&backend), Arc::clone(&stop));
            tokio::spawn(async move {
                let mut seen = Vec::new();
                let mut id = i64::try_from(reader).unwrap();
                while !stop.load(Ordering::Relaxed) {
                    id = id % ITEMS + 1;
                    let ok = match backend.fetch(&item(id)).await {
                        Ok(rows) => rows.rows.len() == 1,
                        Err(e) => {
                            eprintln!("read failed: {e}");
                            false
                        }
                    };
                    seen.push((start.elapsed(), ok));
                }
                seen
            })
        })
        .collect();
    tokio::time::sleep(before).await;
    let stepped_down = start.elapsed();
    let admin = backend.database().client().database("admin");
    let step_down = doc! {
        "replSetStepDown": 10,
        "secondaryCatchUpPeriodSecs": 5,
    };
    // The reply may be lost with the connection; the election is what counts.
    let _ = admin.run_command(step_down).await;
    tokio::time::sleep(after).await;
    stop.store(true, Ordering::Relaxed);
    let mut seen = Vec::new();
    for reader in readers {
        seen.extend(reader.await.unwrap());
    }
    seen.sort_by_key(|(at, _)| *at);
    let mut last_ok = stepped_down;
    let mut longest_gap = Duration::ZERO;
    for (at, ok) in seen.iter().filter(|(at, _)| *at >= stepped_down) {
        if *ok {
            longest_gap = longest_gap.max(at.saturating_sub(last_ok));
            last_ok = *at;
        }
    }
    let failures = seen.iter().filter(|(_, ok)| !ok);
    Outcome {
        ok: seen.iter().filter(|(_, ok)| *ok).count(),
        failed: failures.clone().count(),
        last_failure: failures
            .map(|(at, _)| at.saturating_sub(stepped_down))
            .next_back(),
        longest_gap,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MONGODB_FAILOVER_URL (a disposable 3-member set)"]
async fn native_reads_survive_a_step_down_like_the_driver() {
    let Some(url) = std::env::var("MONGODB_FAILOVER_URL")
        .ok()
        .filter(|u| u.starts_with("mongodb"))
    else {
        eprintln!("failover test skipped: set MONGODB_FAILOVER_URL");
        return;
    };
    let name = format!("siderite_failover_{}", Uuid::new_v4().simple());
    let driver = Arc::new(MongoBackend::connect(&url, &name).await.unwrap());
    let docs: Vec<Document> = (1..=ITEMS).map(item_doc).collect();
    driver
        .database()
        .collection::<Document>("items")
        .insert_many(docs)
        .await
        .unwrap();
    let (before, after) = (Duration::from_secs(2), Duration::from_secs(12));

    let old = primary(&driver).await;
    let on_driver = run(Arc::clone(&driver), before, after).await;
    let new = primary(&driver).await;
    eprintln!("driver: {old} -> {new}: {on_driver:?}");
    assert_ne!(old, new);

    let native = MongoBackend::connect_native(&url, &name).await.unwrap();
    let native = Arc::new(native);
    assert_eq!(native.data_path(), DataPath::Native);
    let old = primary(&native).await;
    assert!(format!("{native:?}").contains(&old), "{native:?}");
    let on_native = run(Arc::clone(&native), before, after).await;
    let new = primary(&native).await;
    eprintln!("native: {old} -> {new}: {on_native:?}");
    assert_ne!(old, new);

    assert!(
        on_native.failed <= on_driver.failed,
        "native {on_native:?} vs driver {on_driver:?}"
    );
    assert!(on_native.ok > 0 && on_driver.ok > 0);
    // The native pool found the new primary: its reads run there again.
    for id in 1..=ITEMS {
        assert_eq!(native.fetch(&item(id)).await.unwrap().rows.len(), 1);
    }
    assert!(format!("{native:?}").contains(&new), "{native:?}");
    driver.database().drop().await.unwrap();
}
