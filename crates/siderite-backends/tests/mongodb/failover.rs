//! Failover under load: `replSetStepDown` on the primary of a three-member
//! replica set while concurrent reads (or inserts) run, once on the driver
//! path and once on the native path.
//!
//! Runs only when `MONGODB_FAILOVER_URL` names such a set (it steps the
//! primary down, so never point it at a shared deployment), for example
//! `mongodb://127.0.0.1:27031,127.0.0.1:27032,127.0.0.1:27033/`
//! `?replicaSet=rs7`; without it the tests return after a note. The native
//! path must fail no more operations than the driver path and must end up
//! on the new primary; inserts (retryable writes) must never be applied
//! twice.

use mongodb::bson::{Document, doc};
use siderite_backends::mongodb::{DataPath, MongoBackend};
use siderite_orm::{Executor, Expr, InsertPlan, QueryPlan, Value, WritePlan};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// A backend the load tasks share.
type Shared = Arc<MongoBackend>;
/// When an operation finished, whether it succeeded, and which task ran it
/// as its how-manyth operation.
type Seen = (Duration, bool, usize, usize);

const TASKS: usize = 16;
const ITEMS: i64 = 100;

/// What one load run saw.
struct Outcome {
    ok: usize,
    failed: usize,
    /// From the step-down to the last failed operation.
    last_failure: Option<Duration>,
    /// Longest stretch after the step-down without a successful operation.
    longest_gap: Duration,
    seen: Vec<Seen>,
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

/// The title the `seq`-th insert of task `task` writes in run `tag`.
fn title(tag: &str, task: usize, seq: usize) -> String {
    format!("{tag}-{task}-{seq}")
}

fn insert_note(title: String) -> WritePlan {
    WritePlan::Insert(InsertPlan {
        table: "notes".into(),
        columns: vec!["title".into()],
        rows: vec![vec![Value::Text(title)]],
        returning: Vec::new(),
    })
}

/// The URL of the disposable set, if one is configured.
fn failover_url() -> Option<String> {
    let url = std::env::var("MONGODB_FAILOVER_URL")
        .ok()
        .filter(|u| u.starts_with("mongodb"));
    if url.is_none() {
        eprintln!("failover test skipped: set MONGODB_FAILOVER_URL");
    }
    url
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

/// Run `op(backend, task, seq)` in [`TASKS`] tasks for `before`, step the
/// primary down, keep going for `after`.
async fn run<F, Fut>(backend: Shared, before: Duration, after: Duration, op: F) -> Outcome
where
    F: Fn(Shared, usize, usize) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = bool> + Send,
{
    let stop = Arc::new(AtomicBool::new(false));
    let start = Instant::now();
    let tasks: Vec<_> = (0..TASKS)
        .map(|task| {
            let (backend, stop, op) = (Arc::clone(&backend), Arc::clone(&stop), op.clone());
            tokio::spawn(async move {
                let mut seen = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let seq = seen.len();
                    let ok = op(Arc::clone(&backend), task, seq).await;
                    seen.push((start.elapsed(), ok, task, seq));
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
    for task in tasks {
        seen.extend(task.await.unwrap());
    }
    seen.sort_by_key(|(at, ..)| *at);
    let mut last_ok = stepped_down;
    let mut longest_gap = Duration::ZERO;
    for (at, ok, ..) in seen.iter().filter(|(at, ..)| *at >= stepped_down) {
        if *ok {
            longest_gap = longest_gap.max(at.saturating_sub(last_ok));
            last_ok = *at;
        }
    }
    let failures = seen.iter().filter(|(_, ok, ..)| !ok);
    Outcome {
        ok: seen.iter().filter(|(_, ok, ..)| *ok).count(),
        failed: failures.clone().count(),
        last_failure: failures
            .map(|(at, ..)| at.saturating_sub(stepped_down))
            .next_back(),
        longest_gap,
        seen,
    }
}

/// Run `load` on the driver path, then on the native path (each with its
/// own step-down), and check that both moved to a new primary and that
/// native failed no more operations.
async fn compare<F, Fut>(url: &str, name: &str, load: F) -> (Shared, Outcome, Outcome)
where
    F: Fn(Shared, usize, usize) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = bool> + Send,
{
    let (before, after) = (Duration::from_secs(2), Duration::from_secs(12));
    let driver = Arc::new(MongoBackend::connect(url, name).await.unwrap());
    let old = primary(&driver).await;
    let on_driver = run(Arc::clone(&driver), before, after, load.clone()).await;
    let new = primary(&driver).await;
    eprintln!("driver: {old} -> {new}: {on_driver:?}");
    assert_ne!(old, new);

    let native = MongoBackend::connect_native(url, name).await.unwrap();
    let native = Arc::new(native);
    assert_eq!(native.data_path(), DataPath::Native);
    let old = primary(&native).await;
    assert!(format!("{native:?}").contains(&old), "{native:?}");
    let on_native = run(Arc::clone(&native), before, after, load).await;
    let new = primary(&native).await;
    eprintln!("native: {old} -> {new}: {on_native:?}");
    assert_ne!(old, new);

    assert!(
        on_native.failed <= on_driver.failed,
        "native {on_native:?} vs driver {on_driver:?}"
    );
    assert!(on_native.ok > 0 && on_driver.ok > 0);
    (native, on_driver, on_native)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MONGODB_FAILOVER_URL (a disposable 3-member set)"]
async fn native_reads_survive_a_step_down_like_the_driver() {
    let Some(url) = failover_url() else {
        return;
    };
    let name = format!("siderite_failover_{}", Uuid::new_v4().simple());
    let setup = MongoBackend::connect(&url, &name).await.unwrap();
    let docs: Vec<Document> = (1..=ITEMS).map(item_doc).collect();
    let items = setup.database().collection::<Document>("items");
    items.insert_many(docs).await.unwrap();
    let read = |backend: Shared, task: usize, seq: usize| async move {
        let id = i64::try_from((task + seq) % 100).unwrap() + 1;
        match backend.fetch(&item(id)).await {
            Ok(rows) => rows.rows.len() == 1,
            Err(e) => {
                eprintln!("read failed: {e}");
                false
            }
        }
    };
    let (native, ..) = compare(&url, &name, read).await;
    // The native pool found the new primary: its reads run there again.
    for id in 1..=ITEMS {
        assert_eq!(native.fetch(&item(id)).await.unwrap().rows.len(), 1);
    }
    let new = primary(&native).await;
    assert!(format!("{native:?}").contains(&new), "{native:?}");
    setup.database().drop().await.unwrap();
}

/// How often each title of run `tag` is stored.
async fn stored_titles(backend: &MongoBackend, tag: &str) -> HashMap<String, usize> {
    let notes = backend.database().collection::<Document>("notes");
    let prefix = format!("^{tag}-");
    let mut cursor = notes
        .find(doc! { "title": { "$regex": prefix } })
        .await
        .unwrap();
    let mut counts = HashMap::new();
    while cursor.advance().await.unwrap() {
        let doc: Document = cursor.deserialize_current().unwrap();
        let title = doc.get_str("title").unwrap().to_owned();
        *counts.entry(title).or_default() += 1;
    }
    counts
}

/// Check run `tag`: every successful insert stored once, no title twice;
/// returns how many failed inserts were applied anyway (ambiguous
/// outcomes, such as a reply lost with its connection).
async fn check_inserts(backend: &MongoBackend, tag: &str, outcome: &Outcome) -> usize {
    let stored = stored_titles(backend, tag).await;
    let twice: Vec<_> = stored.iter().filter(|(_, n)| **n > 1).collect();
    assert!(twice.is_empty(), "{tag}: stored more than once: {twice:?}");
    let mut ambiguous = 0;
    for (_, ok, task, seq) in &outcome.seen {
        let present = stored.contains_key(&title(tag, *task, *seq));
        if *ok {
            assert!(present, "{tag}: confirmed insert {task}/{seq} missing");
        } else if present {
            ambiguous += 1;
        }
    }
    assert_eq!(stored.len(), outcome.ok + ambiguous, "{tag}");
    ambiguous
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MONGODB_FAILOVER_URL (a disposable 3-member set)"]
async fn native_writes_survive_a_step_down_like_the_driver() {
    let Some(url) = failover_url() else {
        return;
    };
    let name = format!("siderite_failover_{}", Uuid::new_v4().simple());
    let write = |backend: Shared, task: usize, seq: usize| async move {
        let tag = path_tag(&backend);
        match backend.execute(&insert_note(title(tag, task, seq))).await {
            Ok(done) => done.rows_affected == 1,
            Err(e) => {
                eprintln!("insert failed: {e}");
                false
            }
        }
    };
    let (native, on_driver, on_native) = compare(&url, &name, write).await;
    let ambiguous = [
        check_inserts(&native, "driver", &on_driver).await,
        check_inserts(&native, "native", &on_native).await,
    ];
    eprintln!("failed but applied (driver, native): {ambiguous:?}");
    // Writes run natively on the new primary.
    let after = native.execute(&insert_note("native-after".into())).await;
    assert_eq!(after.unwrap().rows_affected, 1);
    let new = primary(&native).await;
    assert!(format!("{native:?}").contains(&new), "{native:?}");
    native.database().drop().await.unwrap();
}

/// Which path `backend` runs on, as a title prefix.
fn path_tag(backend: &MongoBackend) -> &'static str {
    match backend.data_path() {
        DataPath::Native => "native",
        _ => "driver",
    }
}
