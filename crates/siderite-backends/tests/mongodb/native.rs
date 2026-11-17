//! The native read path (feature `mongodb-native`): path selection, cursors,
//! cancellation, pool limits, error mapping and transactions.

use crate::fixtures::TestDb;
use mongodb::bson::{Document, doc};
use siderite_backends::mongodb::{DataPath, MongoBackend, NativeOptions};
use siderite_orm::{Backend as _, BackendError, Cells, Executor, Expr};
use siderite_orm::{InsertPlan, OrderDirection, OrmError, QueryError};
use siderite_orm::{QueryPlan, RowSink, SqlType, Value, WritePlan};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

fn url() -> String {
    std::env::var("MONGODB_URL").unwrap()
}

/// `url` with `option` added to its query string.
fn with_option(url: &str, option: &str) -> String {
    if url.contains('?') {
        format!("{url}&{option}")
    } else if url.ends_with('/') {
        format!("{url}?{option}")
    } else {
        format!("{url}/?{option}")
    }
}

fn item_doc(i: i64) -> Document {
    doc! { "_id": i, "n": i }
}

/// A fresh database holding `items` `{_id: i, n: i}` for `i` in `1..=n`.
async fn items(n: i64) -> TestDb {
    let t = TestDb::open().await.unwrap();
    let docs: Vec<Document> = (1..=n).map(item_doc).collect();
    t.backend
        .database()
        .collection::<Document>("items")
        .insert_many(docs)
        .await
        .unwrap();
    t
}

/// A native backend on `t`'s database.
async fn native_on(t: &TestDb, url: &str, o: NativeOptions) -> MongoBackend {
    let name = t.backend.database().name();
    let connect = MongoBackend::connect_native_with(url, name, o);
    let backend = connect.await.unwrap();
    assert_eq!(
        backend.data_path(),
        DataPath::Native,
        "{:?}",
        backend.native_ineligibility()
    );
    backend
}

fn all_items() -> QueryPlan {
    let id = Expr::col("id");
    QueryPlan::from_table("items").order_by(id, OrderDirection::Asc)
}

/// `n` cast to a 64-bit integer (a pipeline).
fn as_long() -> Expr {
    Expr::col("n").cast(SqlType::BigInt)
}

fn item(id: i64) -> QueryPlan {
    QueryPlan::from_table("items").filter(Expr::col("id").eq(Value::Int(id)))
}

fn ids(rows: &[siderite_orm::Row]) -> Vec<i64> {
    rows.iter()
        .map(|r| match r.get("n") {
            Some(Value::Int(n)) => *n,
            other => panic!("{other:?}"),
        })
        .collect()
}

/// Open server cursors on `collection` of `t`'s database.
async fn idle_cursors(t: &TestDb, collection: &str) -> usize {
    let ns = format!("{}.{collection}", t.backend.database().name());
    let admin = t.backend.database().client().database("admin");
    let pipeline = [
        doc! { "$currentOp": { "idleCursors": true, "allUsers": true } },
        doc! { "$match": { "type": "idleCursor", "ns": ns } },
    ];
    let mut cursor = admin.aggregate(pipeline).await.unwrap();
    let mut n = 0;
    while cursor.advance().await.unwrap() {
        n += 1;
    }
    n
}

async fn no_idle_cursors_soon(t: &TestDb, collection: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let open = idle_cursors(t, collection).await;
        if open == 0 {
            return;
        }
        assert!(Instant::now() < deadline, "{open} cursors left open");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn the_path_is_chosen_at_connect() {
    let t = items(3).await;
    let name = t.backend.database().name().to_owned();
    let plain = MongoBackend::connect(&url(), &name).await.unwrap();
    assert_eq!(plain.data_path(), DataPath::Driver);
    assert_eq!(plain.native_ineligibility(), None);

    let native = MongoBackend::connect_native(&url(), &name).await.unwrap();
    assert_eq!(native.data_path(), DataPath::Native);
    assert_eq!(native.native_ineligibility(), None);
    assert_eq!(
        ids(&native.fetch(&all_items()).await.unwrap().rows),
        [1, 2, 3]
    );

    // Ineligible deployments fall back to the driver silently.
    let option = "readPreference=secondaryPreferred";
    let secondary_ok = with_option(&url(), option);
    let fallback = MongoBackend::connect_native(&secondary_ok, &name)
        .await
        .unwrap();
    assert_eq!(fallback.data_path(), DataPath::Driver);
    let why = fallback.native_ineligibility().unwrap();
    assert!(why.contains("read preference"), "{why}");
    assert_eq!(
        ids(&fallback.fetch(&all_items()).await.unwrap().rows),
        [1, 2, 3]
    );
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn large_results_are_drained_with_get_more() {
    let t = items(1000).await;
    let backend = native_on(&t, &url(), NativeOptions::default()).await;
    // 101 documents in the first batch, the rest through getMore.
    let all = backend.fetch(&all_items()).await.unwrap().rows;
    assert_eq!(ids(&all), (1..=1000).collect::<Vec<_>>());
    // A limit above the first batch comes back in one reply.
    let limited = backend.fetch(&all_items().limit(150)).await.unwrap().rows;
    assert_eq!(ids(&limited), (1..=150).collect::<Vec<_>>());
    // Pipelines page the same way.
    let piped = all_items().select(as_long(), Some("n"));
    assert_eq!(ids(&backend.fetch(&piped).await.unwrap().rows).len(), 1000);
    assert_eq!(idle_cursors(&t, "items").await, 0);
    t.cleanup().await;
}

/// A sink that fails at row `fail_at`.
struct FailAt {
    seen: usize,
    fail_at: usize,
}

impl RowSink for FailAt {
    fn push(&mut self, _cells: &mut dyn Cells) -> Result<(), QueryError> {
        self.seen += 1;
        if self.seen == self.fail_at {
            return Err(QueryError::Model("stop".into()));
        }
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn cursors_left_mid_way_are_killed() {
    let t = items(3000).await;
    let backend = native_on(&t, &url(), NativeOptions::default()).await;
    // The sink stops the read in the first batch and in a getMore batch.
    for fail_at in [10, 500] {
        let mut sink = FailAt { seen: 0, fail_at };
        let stopped = backend.fetch_each(&all_items(), &mut sink).await;
        assert!(
            matches!(
                &stopped,
                Err(OrmError::Query(QueryError::Model(m))) if m == "stop"
            ),
            "{stopped:?}"
        );
        assert_eq!(idle_cursors(&t, "items").await, 0);
    }
    // The request future is dropped while the cursor is open.
    let plan = all_items();
    for micros in (0..40).map(|i| 100 + i * 100) {
        let mut sink = FailAt {
            seen: 0,
            fail_at: usize::MAX,
        };
        let read = backend.fetch_each(&plan, &mut sink);
        let wait = Duration::from_micros(micros);
        let _ = tokio::time::timeout(wait, read).await;
    }
    no_idle_cursors_soon(&t, "items").await;
    let all = backend.fetch(&all_items()).await.unwrap().rows;
    assert_eq!(all.len(), 3000);
    t.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn dropped_requests_do_not_poison_the_pool() {
    let t = items(200).await;
    let options = NativeOptions::default().max_connections(3);
    let backend = Arc::new(native_on(&t, &url(), options).await);
    let done = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicUsize::new(0));
    let tasks: Vec<_> = (0..48_i64)
        .map(|task| {
            let (backend, done, cancelled) = (
                Arc::clone(&backend),
                Arc::clone(&done),
                Arc::clone(&cancelled),
            );
            tokio::spawn(async move {
                for round in 0..40_i64 {
                    let id = (task * 40 + round) % 200 + 1;
                    let micros = u64::try_from(round % 8 * 150).unwrap();
                    let wait = Duration::from_micros(micros);
                    let plan = if round % 3 == 0 {
                        all_items()
                    } else {
                        item(id)
                    };
                    let read = backend.fetch(&plan);
                    match tokio::time::timeout(wait, read).await {
                        Ok(rows) => {
                            let rows = rows.unwrap().rows;
                            if round % 3 != 0 {
                                assert_eq!(ids(&rows), [id]);
                            } else {
                                assert_eq!(rows.len(), 200);
                            }
                            done.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            cancelled.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    assert!(cancelled.load(Ordering::Relaxed) > 0);
    assert!(done.load(Ordering::Relaxed) > 0);
    // Every connection left in the pool answers its own request.
    for id in 1..=200 {
        assert_eq!(ids(&backend.fetch(&item(id)).await.unwrap().rows), [id]);
    }
    no_idle_cursors_soon(&t, "items").await;
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn a_full_pool_times_out_cleanly() {
    let t = items(5).await;
    let options = NativeOptions::default()
        .max_connections(1)
        .wait_timeout(Duration::from_millis(50));
    let backend = native_on(&t, &url(), options).await;
    // Polled once, the read sends its request and holds the only
    // connection while it waits for the reply.
    let plan = all_items();
    let mut holder = Box::pin(backend.fetch(&plan));
    assert!(futures_util::poll!(holder.as_mut()).is_pending());
    let started = Instant::now();
    let waited = backend.fetch(&item(1)).await;
    assert!(
        matches!(
            &waited,
            Err(OrmError::Backend(BackendError::Connection(m)))
                if m.contains("became free")
        ),
        "{waited:?}"
    );
    assert!(started.elapsed() < Duration::from_millis(500));
    assert_eq!(holder.await.unwrap().rows.len(), 5);
    assert_eq!(ids(&backend.fetch(&item(1)).await.unwrap().rows), [1]);
    // Dropped while waiting, the read hands its connection back once the
    // reply is in.
    let mut dropped = Box::pin(backend.fetch(&plan));
    assert!(futures_util::poll!(dropped.as_mut()).is_pending());
    drop(dropped);
    assert_eq!(ids(&backend.fetch(&item(2)).await.unwrap().rows), [2]);
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn server_errors_map_like_the_driver_path() {
    let t = items(3).await;
    t.backend
        .database()
        .collection::<Document>("items")
        .insert_one(doc! { "_id": 9_i64, "n": "text" })
        .await
        .unwrap();
    let name = t.backend.database().name().to_owned();
    let driver = MongoBackend::connect(&url(), &name).await.unwrap();
    let native = native_on(&t, &url(), NativeOptions::default()).await;
    let bad_regex = Expr::col("n").regex("(");
    let invalid_regex = QueryPlan::from_table("items").filter(bad_regex);
    let bad_cast = all_items().select(as_long(), Some("n"));
    for plan in [invalid_regex, bad_cast] {
        let expected = driver.fetch(&plan).await.unwrap_err();
        let got = native.fetch(&plan).await.unwrap_err();
        assert!(
            matches!(&got, OrmError::Backend(BackendError::Database(_))),
            "{got:?}"
        );
        // The driver's message also dumps the raw server reply.
        let prefix = |e: &OrmError| {
            let text = e.to_string();
            text.split(", source:").next().unwrap().to_owned()
        };
        assert_eq!(prefix(&got), prefix(&expected));
    }
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn transactions_read_through_the_driver() {
    let t = items(2).await;
    let backend = native_on(&t, &url(), NativeOptions::default()).await;
    let tx = backend.begin(None).await.unwrap();
    let insert = InsertPlan {
        table: "items".into(),
        columns: vec!["id".into(), "n".into()],
        rows: vec![vec![Value::Int(3), Value::Int(3)]],
        returning: Vec::new(),
    };
    tx.execute(&WritePlan::Insert(insert)).await.unwrap();
    // The transaction sees its write; the native path outside it does not.
    assert_eq!(ids(&tx.fetch(&all_items()).await.unwrap().rows), [1, 2, 3]);
    assert_eq!(
        ids(&backend.fetch(&all_items()).await.unwrap().rows),
        [1, 2]
    );
    tx.commit().await.unwrap();
    assert_eq!(
        ids(&backend.fetch(&all_items()).await.unwrap().rows),
        [1, 2, 3]
    );
    t.cleanup().await;
}
