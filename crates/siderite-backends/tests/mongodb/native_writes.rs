//! Native writes (feature `mongodb-native`): results equal to the driver
//! path, batching, write concern, retryable-write sessions, error mapping,
//! cancellation, transactions and fail-point retries.
//!
//! Commands are checked in the database profiler (`system.profile` records
//! an `insert` / `findAndModify` with the command as sent). The fail-point
//! tests need a server started with `--setParameter enableTestCommands=1`
//! and fail without it.

use crate::fixtures::TestDb;
use crate::native::{all_items, ids, items, native_on, url, with_option};
use mongodb::bson::{Bson, Document, doc};
use siderite_backends::mongodb::{MongoBackend, NativeOptions};
use siderite_orm::{Backend as _, BackendError, DeletePlan, ExecResult, Executor, Expr};
use siderite_orm::{InsertPlan, OrmError, UpdatePlan, Value, WritePlan};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use uuid::Uuid;

fn insert(rows: &[(Option<i64>, i64)], returning: &[&'static str]) -> WritePlan {
    let with_ids = rows.iter().all(|(id, _)| id.is_some());
    let columns = if with_ids { vec!["id", "n"] } else { vec!["n"] };
    let rows = rows
        .iter()
        .map(|(id, n)| match id {
            Some(id) if with_ids => vec![Value::Int(*id), Value::Int(*n)],
            _ => vec![Value::Int(*n)],
        })
        .collect();
    WritePlan::Insert(InsertPlan {
        table: "items".into(),
        columns: columns.into_iter().map(Into::into).collect(),
        rows,
        returning: returning.iter().map(|c| (*c).into()).collect(),
    })
}

fn update(filter: Expr, n: Expr, returning: &[&'static str]) -> WritePlan {
    WritePlan::Update(UpdatePlan {
        table: "items".into(),
        assignments: vec![("n".into(), n)],
        filter: Some(filter),
        returning: returning.iter().map(|c| (*c).into()).collect(),
    })
}

fn delete(filter: Expr, returning: &[&'static str]) -> WritePlan {
    WritePlan::Delete(DeletePlan {
        table: "items".into(),
        filter: Some(filter),
        returning: returning.iter().map(|c| (*c).into()).collect(),
    })
}

fn n() -> Expr {
    Expr::col("n")
}

/// A mix of every write shape, with and without `RETURNING`.
fn script() -> Vec<WritePlan> {
    let id = || Expr::col("id");
    vec![
        insert(&[(Some(1001), 1), (Some(1002), 2)], &[]),
        insert(&[(None, 100)], &[]),
        insert(&[(None, 101), (None, 102), (None, 103)], &["id", "n"]),
        insert(&[(Some(50), 50), (Some(51), 51)], &["id"]),
        update(n().ge(Value::Int(101)), Expr::val(7_i64), &[]),
        update(id().eq(Value::Int(50)), n() + Expr::val(10_i64), &["n"]),
        update(n().lt(Value::Int(3)), n() + Expr::val(1_i64), &[]),
        update(id().eq(Value::Int(999)), Expr::val(1_i64), &["n"]),
        delete(id().eq(Value::Int(51)), &[]),
        delete(n().eq(Value::Int(7)), &["id", "n"]),
        delete(id().eq(Value::Int(999)), &["id"]),
    ]
}

async fn run(backend: &MongoBackend, plans: &[WritePlan]) -> Vec<ExecResult> {
    let mut results = Vec::new();
    for plan in plans {
        let done = backend.execute(plan).await;
        results.push(done.unwrap_or_else(|e| panic!("{plan:?}: {e}")));
    }
    results
}

/// Profile every operation of `t`'s database.
async fn profile(t: &TestDb) {
    let db = t.backend.database();
    db.run_command(doc! { "profile": 2_i32 }).await.unwrap();
}

/// The profiled commands of operation `op` on `collection`, oldest first.
async fn profiled(t: &TestDb, op: &str, collection: &str) -> Vec<Document> {
    let db = t.backend.database();
    let ns = format!("{}.{collection}", db.name());
    let filter = doc! { "op": op, "ns": ns };
    let mut cursor = db
        .collection::<Document>("system.profile")
        .find(filter)
        .sort(doc! { "ts": 1_i32 })
        .await
        .unwrap();
    let mut commands = Vec::new();
    while cursor.advance().await.unwrap() {
        let entry: Document = cursor.deserialize_current().unwrap();
        commands.push(entry.get_document("command").unwrap().clone());
    }
    commands
}

/// The `lsid.id` and `txnNumber` of a profiled command.
fn session_of(command: &Document) -> Option<(Bson, i64)> {
    let lsid = command.get_document("lsid").ok()?.get("id")?.clone();
    Some((lsid, command.get_i64("txnNumber").ok()?))
}

/// The error text without the raw server reply the driver may append,
/// and with the database name `db` masked.
fn prefix(e: &OrmError, db: &str) -> String {
    let text = e.to_string().replace(db, "<db>");
    text.split(", source:").next().unwrap().to_owned()
}

/// A driver-path backend on `t`'s database (`t.backend` is native when
/// the suite runs with `SIDERITE_MONGO_NATIVE=1`).
async fn driver_on(t: &TestDb, url: &str) -> MongoBackend {
    let name = t.backend.database().name();
    MongoBackend::connect(url, name).await.unwrap()
}

async fn stored(backend: &MongoBackend) -> Vec<siderite_orm::Row> {
    backend.fetch(&all_items()).await.unwrap().rows
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn native_writes_match_the_driver() {
    let (on_driver, on_native) = (items(0).await, items(0).await);
    let driver = driver_on(&on_driver, &url()).await;
    let native = native_on(&on_native, &url(), NativeOptions::default()).await;
    profile(&on_native).await;
    let expected = run(&driver, &script()).await;
    let got = run(&native, &script()).await;
    assert_eq!(got, expected);
    assert_eq!(stored(&native).await, stored(&driver).await);
    // The inserts and key reservations ran natively: no `$clusterTime`
    // gossip (the driver always sends it), a session on each.
    for (op, collection) in [("insert", "items"), ("command", "siderite_counters")] {
        let commands = profiled(&on_native, op, collection).await;
        assert!(!commands.is_empty(), "{op}");
        for command in commands {
            assert!(!command.contains_key("$clusterTime"), "{command}");
            assert!(session_of(&command).is_some(), "{command}");
        }
    }
    on_driver.cleanup().await;
    on_native.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn bulk_inserts_split_into_batches() {
    let t = items(0).await;
    let options = NativeOptions::default().max_write_batch_size(3);
    let native = native_on(&t, &url(), options).await;
    profile(&t).await;
    let rows: Vec<_> = (1..=10).map(|n| (None, n)).collect();
    let done = native.execute(&insert(&rows, &["id"])).await.unwrap();
    assert_eq!(done.rows_affected, 10);
    let all = native.fetch(&all_items()).await.unwrap().rows;
    assert_eq!(ids(&all), (1..=10).collect::<Vec<_>>());
    // Four commands of 3, 3, 3 and 1 documents on one session, each with
    // the next transaction number.
    let commands = profiled(&t, "insert", "items").await;
    assert_eq!(commands.len(), 4, "{commands:?}");
    let sessions: Vec<_> = commands.iter().map(|c| session_of(c).unwrap()).collect();
    for pair in sessions.windows(2) {
        assert_eq!(pair[0].0, pair[1].0);
        assert_eq!(pair[0].1 + 1, pair[1].1);
    }
    // Over the message size limit (48 MB): two commands.
    let big = "x".repeat(1_000_000);
    let docs = (0..60_i64).map(|i| doc! { "_id": 100 + i, "n": i, "pad": &big });
    let wide = InsertPlan {
        table: "wide".into(),
        columns: vec!["id".into(), "n".into(), "pad".into()],
        rows: docs
            .map(|d| {
                vec![
                    Value::Int(d.get_i64("_id").unwrap()),
                    Value::Int(d.get_i64("n").unwrap()),
                    Value::Text(big.clone()),
                ]
            })
            .collect(),
        returning: Vec::new(),
    };
    let unbatched = native_on(&t, &url(), NativeOptions::default()).await;
    let done = unbatched.execute(&WritePlan::Insert(wide)).await.unwrap();
    assert_eq!(done.rows_affected, 60);
    assert_eq!(profiled(&t, "insert", "wide").await.len(), 2);
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn write_errors_map_like_the_driver() {
    let (on_driver, on_native) = (items(3).await, items(3).await);
    let driver = driver_on(&on_driver, &url()).await;
    let options = NativeOptions::default().max_write_batch_size(2);
    let native = native_on(&on_native, &url(), options).await;
    for t in [&on_driver, &on_native] {
        t.backend.create_unique_index("items", "n").await.unwrap();
    }
    let names = [&on_driver, &on_native].map(|t| t.backend.database().name().to_owned());
    let same = |plan: WritePlan| {
        let (driver, native, names) = (&driver, &native, &names);
        async move {
            let expected = driver.execute(&plan).await.unwrap_err();
            let got = native.execute(&plan).await.unwrap_err();
            assert!(
                matches!(&got, OrmError::Backend(BackendError::Constraint(_))),
                "{got:?}"
            );
            assert_eq!(prefix(&got, &names[1]), prefix(&expected, &names[0]));
            assert!(got.to_string().contains("E11000"), "{got}");
        }
    };
    // A duplicate `_id`, and one in the second batch (index 2 of the
    // insert either way).
    same(insert(&[(Some(1), 10)], &[])).await;
    same(insert(
        &[(Some(20), 20), (Some(21), 21), (Some(2), 22)],
        &[],
    ))
    .await;
    // A duplicate in a unique index, by insert and by update.
    same(insert(&[(Some(30), 1)], &[])).await;
    let id = Expr::col("id");
    same(update(id.eq(Value::Int(3)), Expr::val(1_i64), &[])).await;
    // Ordered: the documents before the duplicate are in, none after.
    assert_eq!(ids(&stored(&native).await), [1, 2, 3, 20, 21]);
    assert_eq!(stored(&native).await, stored(&driver).await);
    on_driver.cleanup().await;
    on_native.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn write_concern_is_sent_like_the_driver() {
    let cases = [
        "",
        "w=1",
        "w=majority&wtimeoutMS=5000",
        "journal=true",
        "w=1&journal=false",
    ];
    for option in cases {
        let url = if option.is_empty() {
            url()
        } else {
            with_option(&url(), option)
        };
        let t = items(0).await;
        let driver = driver_on(&t, &url).await;
        let native = native_on(&t, &url, NativeOptions::default()).await;
        profile(&t).await;
        let plan = insert(&[(None, 1)], &[]);
        driver.execute(&plan).await.unwrap();
        native.execute(&plan).await.unwrap();
        let commands = profiled(&t, "insert", "items").await;
        assert_eq!(commands.len(), 2, "{option}: {commands:?}");
        let (sent_by_driver, sent_natively) = (&commands[0], &commands[1]);
        assert!(sent_by_driver.contains_key("$clusterTime"));
        assert!(!sent_natively.contains_key("$clusterTime"));
        assert_eq!(
            sent_natively.get("writeConcern"),
            sent_by_driver.get("writeConcern"),
            "{option}"
        );
        assert_eq!(sent_natively.get("ordered"), Some(&Bson::Boolean(true)));
        t.cleanup().await;
    }
    // A write concern the server cannot satisfy fails every write the same
    // way on both paths (so updates and deletes carry it too).
    let unsatisfiable = with_option(&url(), "w=7");
    let t = items(2).await;
    let name = t.backend.database().name().to_owned();
    let driver = driver_on(&t, &unsatisfiable).await;
    let native = native_on(&t, &unsatisfiable, NativeOptions::default()).await;
    let id = || Expr::col("id");
    let pairs = [
        (insert(&[(Some(9), 9)], &[]), insert(&[(Some(10), 9)], &[])),
        (insert(&[(None, 9)], &[]), insert(&[(None, 9)], &[])),
        (
            update(id().eq(Value::Int(1)), Expr::val(5_i64), &[]),
            update(id().eq(Value::Int(1)), Expr::val(6_i64), &[]),
        ),
        (
            delete(id().eq(Value::Int(1)), &[]),
            delete(id().eq(Value::Int(2)), &[]),
        ),
    ];
    for (on_driver, on_native) in pairs {
        let expected = driver.execute(&on_driver).await.unwrap_err();
        let got = native.execute(&on_native).await.unwrap_err();
        assert_eq!(
            prefix(&got, &name),
            prefix(&expected, &name),
            "{on_native:?}"
        );
    }
    // Unacknowledged writes stay on the driver, which rejects them.
    let unacknowledged = with_option(&url(), "w=0");
    let native = native_on(&t, &unacknowledged, NativeOptions::default()).await;
    assert!(
        format!("{native:?}").contains("writes: false"),
        "{native:?}"
    );
    let refused = native.execute(&insert(&[(Some(8), 8)], &[])).await;
    assert!(
        refused
            .unwrap_err()
            .to_string()
            .contains("Unacknowledged write concerns are not supported")
    );
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn sessions_follow_retry_writes() {
    let t = items(0).await;
    profile(&t).await;
    let native = native_on(&t, &url(), NativeOptions::default()).await;
    for n in 101..=103 {
        native.execute(&insert(&[(Some(n), n)], &[])).await.unwrap();
    }
    // Sequential writes reuse one session with growing txnNumbers.
    let commands = profiled(&t, "insert", "items").await;
    let sessions: Vec<_> = commands.iter().map(|c| session_of(c).unwrap()).collect();
    assert_eq!(sessions.len(), 3);
    assert!(
        sessions
            .windows(2)
            .all(|p| p[0].0 == p[1].0 && p[0].1 < p[1].1)
    );
    // retryWrites=false: no session, no transaction number.
    let url = with_option(&url(), "retryWrites=false");
    let plain = native_on(&t, &url, NativeOptions::default()).await;
    plain.execute(&insert(&[(None, 7)], &[])).await.unwrap();
    let commands = profiled(&t, "insert", "items").await;
    let last = commands.last().unwrap();
    assert!(!last.contains_key("lsid") && !last.contains_key("txnNumber"));
    let counters = profiled(&t, "command", "siderite_counters").await;
    assert!(session_of(counters.last().unwrap()).is_none());
    t.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn cancelled_writes_do_not_poison_the_pool() {
    let t = items(0).await;
    let options = NativeOptions::default().max_connections(3);
    let backend = Arc::new(native_on(&t, &url(), options).await);
    let done = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicUsize::new(0));
    let tasks: Vec<_> = (0..32_i64)
        .map(|task| {
            let (backend, done, cancelled) = (
                Arc::clone(&backend),
                Arc::clone(&done),
                Arc::clone(&cancelled),
            );
            tokio::spawn(async move {
                let mut confirmed = Vec::new();
                for round in 0..30_i64 {
                    let id = task * 1000 + round + 1;
                    let micros = u64::try_from(round % 6 * 200).unwrap();
                    let wait = Duration::from_micros(micros);
                    let plan = insert(&[(Some(id), id)], &[]);
                    match tokio::time::timeout(wait, backend.execute(&plan)).await {
                        Ok(result) => {
                            assert_eq!(result.unwrap().rows_affected, 1);
                            confirmed.push(id);
                            done.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            cancelled.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                confirmed
            })
        })
        .collect();
    let mut confirmed = Vec::new();
    for task in tasks {
        confirmed.extend(task.await.unwrap());
    }
    assert!(cancelled.load(Ordering::Relaxed) > 0);
    assert!(done.load(Ordering::Relaxed) > 0);
    // Every confirmed insert is stored, once.
    let stored = ids(&backend.fetch(&all_items()).await.unwrap().rows);
    for id in &confirmed {
        assert_eq!(stored.iter().filter(|s| *s == id).count(), 1, "{id}");
    }
    // Every connection and session left answers its own request.
    for id in 100_001..=100_050_i64 {
        let plan = insert(&[(Some(id), id)], &["n"]);
        let done = backend.execute(&plan).await.unwrap();
        assert_eq!(done.returning[0].get("n"), Some(&Value::Int(id)));
        let read = backend.fetch(&crate::native::item(id)).await.unwrap();
        assert_eq!(ids(&read.rows), [id]);
    }
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn transactions_write_through_the_driver() {
    let t = items(2).await;
    let backend = native_on(&t, &url(), NativeOptions::default()).await;
    let id = || Expr::col("id");
    for commit in [true, false] {
        let tx = backend.begin(None).await.unwrap();
        tx.execute(&insert(&[(Some(3), 3)], &[])).await.unwrap();
        let bump = update(id().eq(Value::Int(1)), n() + Expr::val(10_i64), &[]);
        tx.execute(&bump).await.unwrap();
        tx.execute(&delete(id().eq(Value::Int(2)), &[]))
            .await
            .unwrap();
        assert_eq!(ids(&tx.fetch(&all_items()).await.unwrap().rows), [11, 3]);
        // Outside the transaction (the native path) nothing changed yet.
        let outside = backend.fetch(&all_items()).await.unwrap().rows;
        assert_eq!(ids(&outside), [1, 2]);
        if commit {
            tx.commit().await.unwrap();
            let after = backend.fetch(&all_items()).await.unwrap().rows;
            assert_eq!(ids(&after), [11, 3]);
            // Back to the starting rows for the rollback round.
            backend
                .execute(&update(id().eq(Value::Int(1)), Expr::val(1_i64), &[]))
                .await
                .unwrap();
            backend
                .execute(&delete(id().eq(Value::Int(3)), &[]))
                .await
                .unwrap();
            backend
                .execute(&insert(&[(Some(2), 2)], &[]))
                .await
                .unwrap();
        } else {
            tx.rollback().await.unwrap();
            let after = backend.fetch(&all_items()).await.unwrap().rows;
            assert_eq!(ids(&after), [1, 2]);
        }
    }
    t.cleanup().await;
}

/// Configure fail point `failCommand` (needs `enableTestCommands`).
async fn fail_point(t: &TestDb, mode: Bson, data: Document) {
    let admin = t.backend.database().client().database("admin");
    let set = doc! { "configureFailPoint": "failCommand", "mode": mode, "data": data };
    if let Err(e) = admin.run_command(set).await {
        panic!(
            "fail points need a server started with --setParameter \
             enableTestCommands=1: {e}"
        );
    }
}

async fn fail_point_off(t: &TestDb) {
    fail_point(t, Bson::String("off".into()), Document::new()).await;
}

/// The server's count of retried retryable writes it had already applied.
async fn retried_commands(t: &TestDb) -> i64 {
    let admin = t.backend.database().client().database("admin");
    let status = admin
        .run_command(doc! { "serverStatus": 1_i32 })
        .await
        .unwrap();
    let transactions = status.get_document("transactions").unwrap();
    match transactions.get("retriedCommandsCount") {
        Some(Bson::Int64(n)) => *n,
        Some(Bson::Int32(n)) => i64::from(*n),
        other => panic!("{other:?}"),
    }
}

/// A native backend whose connections identify as `app` (fail points
/// match on it, sparing the driver's own commands).
async fn native_app(t: &TestDb, app: &str, extra: &str) -> MongoBackend {
    let mut url = with_option(&url(), &format!("appName={app}"));
    if !extra.is_empty() {
        url = with_option(&url, extra);
    }
    native_on(t, &url, NativeOptions::default()).await
}

#[tokio::test]
#[ignore = "requires MONGODB_URL on a server with enableTestCommands=1"]
async fn retryable_writes_retry_once_with_the_same_session() {
    let t = items(0).await;
    let app = format!("t8-{}", Uuid::new_v4().simple());
    let native = native_app(&t, &app, "").await;
    profile(&t).await;
    let once = || doc! { "times": 1_i32 };
    // The write is applied, then its reply carries a retryable write
    // concern error: the retry with the same lsid / txnNumber is answered
    // from the server's record (a new txnNumber would hit the duplicate
    // `_id`).
    let wce = doc! {
        "failCommands": ["insert"], "appName": &app,
        "writeConcernError": {
            "code": 91_i32, "codeName": "ShutdownInProgress",
            "errmsg": "Replication is being shut down",
            "errorLabels": ["RetryableWriteError"],
        },
    };
    fail_point(&t, Bson::Document(once()), wce).await;
    let retried = retried_commands(&t).await;
    let done = native
        .execute(&insert(&[(Some(101), 1)], &[]))
        .await
        .unwrap();
    assert_eq!(done.rows_affected, 1);
    // The server saw the retry of a write it had applied (it answers such
    // a retry without running or profiling it again).
    assert_eq!(retried_commands(&t).await, retried + 1);
    assert_eq!(profiled(&t, "insert", "items").await.len(), 1);
    // Not primary, before the write: retried once and applied once, for
    // the key counter and the insert.
    for command in ["findAndModify", "insert"] {
        let not_primary = doc! {
            "failCommands": [command], "appName": &app, "errorCode": 10107_i32,
        };
        fail_point(&t, Bson::Document(once()), not_primary).await;
        let row = if command == "insert" { Some(105) } else { None };
        let done = native.execute(&insert(&[(row, 2)], &["id"])).await;
        assert_eq!(done.unwrap().rows_affected, 1, "{command}");
    }
    // A connection closed under the write.
    let closed = doc! {
        "failCommands": ["insert"], "appName": &app, "closeConnection": true,
    };
    fail_point(&t, Bson::Document(once()), closed).await;
    native
        .execute(&insert(&[(Some(103), 3)], &[]))
        .await
        .unwrap();
    let stored = ids(&native.fetch(&all_items()).await.unwrap().rows);
    // By id: the generated key 1, then 101, 103, 105.
    assert_eq!(stored, [2, 1, 3, 2]);
    // Twice in a row: the retry fails too, and its error is reported.
    let twice = doc! {
        "failCommands": ["insert"], "appName": &app, "errorCode": 10107_i32,
    };
    fail_point(&t, Bson::Document(doc! { "times": 2_i32 }), twice).await;
    let failed = native.execute(&insert(&[(Some(104), 4)], &[])).await;
    assert!(failed.unwrap_err().to_string().contains("10107"));
    fail_point_off(&t).await;
    native
        .execute(&insert(&[(Some(104), 4)], &[]))
        .await
        .unwrap();
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires MONGODB_URL on a server with enableTestCommands=1"]
async fn non_retryable_writes_are_not_retried() {
    let t = items(2).await;
    let app = format!("t8-{}", Uuid::new_v4().simple());
    let id = || Expr::col("id");
    let once = || Bson::Document(doc! { "times": 1_i32 });
    // Multi-document updates and deletes.
    let native = native_app(&t, &app, "").await;
    let not_primary = doc! {
        "failCommands": ["update", "delete"], "appName": &app,
        "errorCode": 10107_i32,
    };
    let bump = update(id().eq(Value::Int(1)), Expr::val(5_i64), &[]);
    fail_point(&t, once(), not_primary.clone()).await;
    assert!(native.execute(&bump).await.is_err());
    assert_eq!(native.execute(&bump).await.unwrap().rows_affected, 1);
    fail_point(&t, once(), not_primary).await;
    let gone = delete(id().eq(Value::Int(2)), &[]);
    assert!(native.execute(&gone).await.is_err());
    assert_eq!(native.execute(&gone).await.unwrap().rows_affected, 1);
    // Any write with retryWrites=false.
    let plain = native_app(&t, &app, "retryWrites=false").await;
    let not_primary = doc! {
        "failCommands": ["insert"], "appName": &app, "errorCode": 10107_i32,
    };
    fail_point(&t, once(), not_primary).await;
    let failed = plain.execute(&insert(&[(Some(3), 3)], &[])).await;
    assert!(failed.unwrap_err().to_string().contains("10107"));
    fail_point_off(&t).await;
    let stored = ids(&plain.fetch(&all_items()).await.unwrap().rows);
    assert_eq!(stored, [5]);
    t.cleanup().await;
}
