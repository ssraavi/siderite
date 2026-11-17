//! MongoDB: a primary-key read, a page of 20 todos and a single-document
//! insert through the ORM, through the raw driver, and as a hand-rolled
//! `OP_MSG` round trip (the floor), plus the compile-only cost of the read.
//!
//! Needs a live server: set `MONGODB_URL` (for example
//! `mongodb://127.0.0.1:27017/?replicaSet=rs0`). Without it the binary
//! prints a note and runs nothing. Every variant works in the
//! `siderite_bench` database, which is dropped and seeded with 100 todos
//! (and their key counter, as the benchmark runner seeds them) on start.
//! Before measuring, the commands the ORM sends for one `get` and one
//! `create` are printed, and the read's compiled `find` is checked against
//! them.
//!
//! * `mongo_get_pk`: `orm` is `Todo::objects(&db).get(Todo::id.eq(1))`;
//!   `driver_compiled` runs the `find` the ORM compiles (filter and
//!   options) through the driver, so it mirrors what the ORM sends;
//!   `driver_aggregate` is the reference for the former read path, the
//!   pipeline the ORM compiled before reads became `find`s
//!   (`[{$match: {_id: {$eq: 1}}}, {$limit: 2}]`); `driver_find_one` is
//!   `find_one({_id: 1})`; `opmsg_find` is a `find` with `limit: 1` and
//!   `singleBatch` on one raw connection; `opmsg_find_orm_shape` replays on
//!   it the ORM's `find` as the driver sent it but without `lsid` and
//!   `$clusterTime`, and `opmsg_find_as_driver` replays it with both (on a
//!   fresh session id), so the gap between the two is the server's cost of
//!   the implicit session and cluster-time gossip and the gap from
//!   `opmsg_find_as_driver` to `driver_compiled` is the driver's own;
//!   `compile` builds the queryset and compiles it, without I/O;
//!   `orm_native` is `orm` on a backend opened with
//!   `MongoBackend::connect_native` (the native `OP_MSG` read path; skipped
//!   with a note when the deployment does not qualify).
//! * `mongo_list`: `orm` is `Todo::objects(&db).limit(20).all()`, the
//!   first page `GET /todos` reads (newest first); `driver_compiled` runs
//!   the `find` the ORM compiles and collects every document as a
//!   `Document`; `driver_raw` runs the same `find` and only walks each
//!   reply document's raw BSON, the floor for decoding through the driver;
//!   `orm_native` is `orm` on the native read path.
//! * `mongo_insert`: `orm` is `create` (keys come from blocks of 64 reserved
//!   with one counter update, so most inserts are one round trip);
//!   `driver_counter_insert` reserves each key from the counter
//!   (`findAndModify`) and then inserts, as the ORM did with one key per
//!   counter update; `driver_insert_client_key` and `opmsg_insert` insert
//!   with a key chosen by the client. Ids starting `w_default/` leave the
//!   write concern to the server (majority, journaled, on a default replica
//!   set); `w1/` ids set `w=1` on the URL, or in the raw command.
//!
//! All variants run one request at a time on a current-thread Tokio
//! runtime, so each time is the latency of one request.

#[path = "common/opmsg.rs"]
mod opmsg;

use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, Criterion};
use mongodb::bson::{Bson, Document, RawDocument, Uuid, doc};
use mongodb::event::EventHandler;
use mongodb::event::command::CommandEvent;
use mongodb::options::{ClientOptions, ReturnDocument};
use mongodb::{Client, Collection, Cursor};
use siderite::orm::Db;
use siderite::prelude::*;
use siderite_backends::mongodb::compile::{CompiledQuery, FindQuery, Read, compile_query};
use siderite_backends::mongodb::{DataPath, Keys, MongoBackend};
use std::fmt::Display;
use std::hint::black_box;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;

/// Error type of every fallible step.
type Res<T> = Result<T, opmsg::WireError>;

/// Database every variant uses; dropped and reseeded on start.
const DATABASE: &str = "siderite_bench";
/// Collection of [`Todo`].
const TODOS: &str = "todos";
/// Collection of the ORM's key counters.
const COUNTERS: &str = "siderite_counters";
/// Todos seeded with keys `1..=SEEDED`; the counter starts there.
const SEEDED: i64 = 100;
/// The key every read fetches, as `GET /todos/1` does.
const LOOKUP_ID: i64 = 1;
/// Todos on the page `GET /todos` lists.
const PAGE: usize = 20;
/// Title of every inserted todo, as the HTTP benchmark posts it.
const TITLE: &str = "x";
/// Client-side keys of each target start at a multiple of this, far above
/// the counter's range.
const CLIENT_KEY_SPACING: i64 = 1 << 40;
/// Commands worth printing; drivers also send `hello`, `endSessions` etc.
const DATA_COMMANDS: [&str; 4] = ["aggregate", "find", "findAndModify", "insert"];

/// The todo model of the benchmark app (`benchmarks/siderite_todo`).
#[derive(Debug, Clone, PartialEq, Model)]
#[model(table = "todos", ordering = ["-id"])]
struct Todo {
    #[field(primary_key, auto)]
    id: i64,
    title: String,
    done: bool,
}

/// The value of `result`, or a panic naming the error: the workspace
/// denies `unwrap` and `expect`, and a failed request must not be timed as
/// if it succeeded.
fn ok<T, E: Display>(result: Result<T, E>) -> T {
    match result {
        Ok(value) => value,
        Err(error) => panic!("mongo benchmark failed: {error}"),
    }
}

/// `url` with the query parameter `param` (`name=value`) added.
fn with_param(url: &str, param: &str) -> String {
    if url.contains('?') {
        format!("{url}&{param}")
    } else if url.ends_with('/') {
        format!("{url}?{param}")
    } else {
        format!("{url}/?{param}")
    }
}

/// One write-concern setting: the ORM, the driver and a raw connection on
/// the same server and database.
struct Target {
    /// Prefix of this target's benchmark ids.
    label: &'static str,
    /// The ORM handle.
    db: Db,
    /// The ORM on the native read path, when the deployment qualifies.
    native: Option<Db>,
    /// The todo collection through the driver.
    todos: Collection<Document>,
    /// The ORM's key counters through the driver.
    counters: Collection<Document>,
    /// The raw `OP_MSG` connection.
    wire: opmsg::Conn,
    /// `writeConcern` of raw `insert` commands; `None` uses the server's.
    write_concern: Option<Document>,
    /// Next client-side key.
    next_key: AtomicI64,
}

impl Target {
    /// Connect to `url` for ids prefixed `label`, with client-side keys
    /// from `slot * CLIENT_KEY_SPACING`.
    async fn open(
        label: &'static str,
        url: &str,
        write_concern: Option<Document>,
        slot: i64,
    ) -> Res<Self> {
        let options = ClientOptions::parse(url).await?;
        let addr = options.hosts.first().ok_or("URL names no host")?;
        let wire = opmsg::Conn::connect(&addr.to_string()).await?;
        let client = Client::with_options(options)?;
        let database = client.database(DATABASE);
        let native = MongoBackend::connect_native(url, DATABASE).await?;
        let native = if native.data_path() == DataPath::Native {
            Some(Db::new(native))
        } else {
            println!(
                "{label}: orm_native skipped, native path ineligible: {}",
                native.native_ineligibility().unwrap_or_default()
            );
            None
        };
        Ok(Self {
            label,
            db: Db::new(MongoBackend::from_client(client, DATABASE)),
            native,
            todos: database.collection(TODOS),
            counters: database.collection(COUNTERS),
            wire,
            write_concern,
            next_key: AtomicI64::new(slot * CLIENT_KEY_SPACING),
        })
    }

    /// A key no other insert uses.
    fn client_key(&self) -> i64 {
        self.next_key.fetch_add(1, Ordering::Relaxed)
    }
}

/// The document stored for the inserted todo with key `id`.
fn todo_doc(id: i64) -> Document {
    doc! { "_id": id, "title": TITLE, "done": false }
}

/// Drop the database and seed it as `benchmarks/database_fixtures.py`
/// does: todos `1..=SEEDED` and a counter at `SEEDED`.
async fn seed(target: &Target) -> Res<()> {
    target.todos.client().database(DATABASE).drop().await?;
    let docs = (1..=SEEDED).map(|i| {
        doc! { "_id": i, "title": format!("Todo task #{i}"), "done": i % 2 == 0 }
    });
    target.todos.insert_many(docs).await?;
    target
        .counters
        .insert_one(doc! { "_id": TODOS, "seq": SEEDED })
        .await?;
    Ok(())
}

/// The commands the ORM sends for one `get` and one `create` on `url`,
/// observed through the driver's command monitoring.
async fn observe_orm(url: &str) -> Res<(Db, Vec<Document>)> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let mut options = ClientOptions::parse(url).await?;
    options.command_event_handler = Some(EventHandler::callback(move |event| {
        if let CommandEvent::Started(started) = event
            && DATA_COMMANDS.contains(&started.command_name.as_str())
            && let Ok(mut seen) = sink.lock()
        {
            seen.push(started.command);
        }
    }));
    let db = Db::new(MongoBackend::from_client(
        Client::with_options(options)?,
        DATABASE,
    ));
    orm_get(&db).await?;
    orm_create(&db).await?;
    let commands = seen.lock().map_err(|e| e.to_string())?.clone();
    Ok((db, commands))
}

/// Print what the ORM sends on `url` (without session and cluster-time
/// fields) and check the read is the `find` [`compile_get`] builds, so
/// `driver_compiled` and `compile` time the same query. Returns that
/// `find` exactly as the driver sent it, session and cluster time included.
async fn report_orm_commands(label: &str, url: &str) -> Res<Document> {
    let (db, commands) = observe_orm(url).await?;
    let mut sent = None;
    let mut as_sent = None;
    for command in commands {
        let mut shown = command.clone();
        shown.remove("lsid");
        shown.remove("$clusterTime");
        println!("ORM command ({label}): {shown}");
        if shown.contains_key("aggregate") {
            return Err(format!("the ORM read is not a find: {shown}").into());
        }
        if shown.contains_key("find") {
            sent = Some(shown);
            as_sent = Some(command);
        }
    }
    let (sent, as_sent) = sent.zip(as_sent).ok_or("the ORM sent no find")?;
    let ours = compiled_find(&compile_get(&db, &Keys::default(), LOOKUP_ID)?)?;
    let options = ours.options();
    let int = |key: &str| match sent.get(key) {
        Some(Bson::Int32(n)) => Some(i64::from(*n)),
        Some(Bson::Int64(n)) => Some(*n),
        _ => None,
    };
    let matches = sent.get_document("filter").ok() == Some(&ours.filter)
        && sent.get_document("projection").ok() == ours.projection.as_ref()
        && sent.get_document("sort").ok() == ours.sort.as_ref()
        && int("skip") == ours.skip.and_then(|n| i64::try_from(n).ok())
        && int("limit") == ours.limit
        // The driver sends one more than an equal batch size.
        && int("batchSize") == options.batch_size.map(|n| i64::from(n) + 1);
    if !matches {
        return Err(format!("ORM sent {sent}, bench compiles {ours:?}").into());
    }
    Ok(as_sent)
}

/// The `find` of a compiled read.
fn compiled_find(compiled: &CompiledQuery) -> Res<FindQuery> {
    match &compiled.read {
        Read::Find(find) => Ok((**find).clone()),
        Read::Pipeline(pipeline) => Err(format!("the get compiles to {pipeline:?}").into()),
    }
}

/// The read `get(Todo::id.eq(id))` sends: the filter and `limit(2)`, with
/// the model's ordering dropped as a primary-key `get` drops it.
fn get_queryset(db: &Db, id: i64) -> QuerySet<Todo> {
    Todo::objects(db)
        .filter(Todo::id.eq(id))
        .limit(2)
        .order_by(Vec::new())
}

/// Build the [`get_queryset`] for `id` and compile it.
fn compile_get(db: &Db, keys: &Keys, id: i64) -> Res<CompiledQuery> {
    Ok(compile_query(get_queryset(db, id).plan(), keys)?)
}

/// Variant `orm` of `mongo_get_pk`.
async fn orm_get(db: &Db) -> Res<Todo> {
    Ok(Todo::objects(db).get(Todo::id.eq(LOOKUP_ID)).await?)
}

/// Variant `driver_compiled`: the compiled `find` with its options, every
/// document collected as the ORM collects them.
async fn driver_compiled(todos: &Collection<Document>, find: &FindQuery) -> Res<Vec<Document>> {
    let cursor = todos
        .find(find.filter.clone())
        .with_options(find.options())
        .await?;
    collect(cursor).await
}

/// The pipeline the ORM compiled for a primary-key `get` before simple
/// reads became `find`s.
fn former_pipeline() -> Vec<Document> {
    vec![
        doc! { "$match": { "_id": { "$eq": LOOKUP_ID } } },
        doc! { "$limit": 2_i64 },
    ]
}

/// Variant `driver_aggregate`: [`former_pipeline`] through `aggregate`,
/// every document collected as the ORM collected them.
async fn driver_aggregate(
    todos: &Collection<Document>,
    pipeline: &[Document],
) -> Res<Vec<Document>> {
    let cursor = todos.aggregate(pipeline.to_vec()).await?;
    collect(cursor).await
}

/// Every document of `cursor`.
async fn collect(mut cursor: Cursor<Document>) -> Res<Vec<Document>> {
    let mut out = Vec::new();
    while cursor.advance().await? {
        out.push(cursor.deserialize_current()?);
    }
    Ok(out)
}

/// Variant `driver_find_one`.
async fn driver_find_one(todos: &Collection<Document>) -> Res<Document> {
    let found = todos.find_one(doc! { "_id": LOOKUP_ID }).await?;
    Ok(found.ok_or("todo not found")?)
}

/// Variant `opmsg_find`: a `find` whose single batch must hold the todo.
async fn opmsg_find(wire: &opmsg::Conn) -> Res<()> {
    let command = doc! {
        "find": TODOS,
        "filter": { "_id": LOOKUP_ID },
        "limit": 1_i32,
        "singleBatch": true,
        "$db": DATABASE,
    };
    wire.run(&command, check_first_todo).await
}

/// Pass when the first batch of a `find` reply starts with the todo
/// [`LOOKUP_ID`].
fn check_first_todo(reply: &RawDocument) -> Res<()> {
    let batch = reply.get_document("cursor")?.get_array("firstBatch")?;
    match batch.get_document(0)?.get_i64("_id")? {
        LOOKUP_ID => Ok(()),
        _ => Err("find returned another todo".into()),
    }
}

/// The ORM's `find` as the driver sent it, on a fresh session id when
/// `session` is true and without `lsid` and `$clusterTime` when it is
/// false: the commands of the `opmsg_find_as_driver` and
/// `opmsg_find_orm_shape` variants.
fn replayed_find(as_sent: &Document, session: bool) -> Document {
    let mut command = as_sent.clone();
    if session {
        command.insert("lsid", doc! { "id": Uuid::new() });
    } else {
        command.remove("lsid");
        command.remove("$clusterTime");
    }
    command
}

/// Variants `opmsg_find_orm_shape` and `opmsg_find_as_driver`: send a copy
/// of `command` (from [`replayed_find`]), as a client builds each command
/// anew.
async fn opmsg_replay(wire: &opmsg::Conn, command: &Document) -> Res<()> {
    wire.run(&command.clone(), check_first_todo).await
}

/// The read `GET /todos` sends: the first page in the model's ordering.
fn list_queryset(db: &Db) -> QuerySet<Todo> {
    Todo::objects(db).limit(PAGE as u64)
}

/// Variant `orm` of `mongo_list`.
async fn orm_list(db: &Db) -> Res<Vec<Todo>> {
    Ok(list_queryset(db).all().await?)
}

/// Variant `driver_raw` of `mongo_list`: the compiled `find`, walking
/// every field of each document without decoding it.
async fn driver_raw(todos: &Collection<Document>, find: &FindQuery) -> Res<usize> {
    let mut cursor = todos
        .find(find.filter.clone())
        .with_options(find.options())
        .await?;
    let mut fields = 0;
    while cursor.advance().await? {
        for field in cursor.current() {
            black_box(field?);
            fields += 1;
        }
    }
    Ok(fields)
}

/// Variant `orm` of `mongo_insert`.
async fn orm_create(db: &Db) -> Res<Todo> {
    let todo = Todo {
        id: 0,
        title: TITLE.to_owned(),
        done: false,
    };
    Ok(Todo::objects(db).create(todo).await?)
}

/// Variant `driver_counter_insert`: reserve one key from the counter (the
/// ORM's former per-insert path, kept as the reference), then insert.
async fn driver_counter_insert(target: &Target) -> Res<i64> {
    let counter = target
        .counters
        .find_one_and_update(doc! { "_id": TODOS }, doc! { "$inc": { "seq": 1_i64 } })
        .upsert(true)
        .return_document(ReturnDocument::After)
        .await?
        .ok_or("no key counter")?;
    let id = counter.get_i64("seq")?;
    target.todos.insert_one(todo_doc(id)).await?;
    Ok(id)
}

/// Variant `driver_insert_client_key`.
async fn driver_insert(target: &Target) -> Res<()> {
    target
        .todos
        .insert_one(todo_doc(target.client_key()))
        .await?;
    Ok(())
}

/// Variant `opmsg_insert`: an `insert` that must report one document and
/// no write errors.
async fn opmsg_insert(target: &Target) -> Res<()> {
    let mut command = doc! {
        "insert": TODOS,
        "documents": [todo_doc(target.client_key())],
        "$db": DATABASE,
    };
    if let Some(concern) = &target.write_concern {
        command.insert("writeConcern", concern.clone());
    }
    target
        .wire
        .run(&command, |reply| {
            if reply.get_i32("n")? == 1 && reply.get("writeErrors")?.is_none() {
                Ok(())
            } else {
                Err("insert was not acknowledged".into())
            }
        })
        .await
}

/// Time `op` as `id` in `group`; a failed run panics.
fn bench_op<T, F, Fut>(group: &mut BenchmarkGroup<'_, WallTime>, rt: &Runtime, id: &str, op: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Res<T>>,
{
    group.bench_function(id, |b| {
        b.to_async(rt).iter(|| {
            let pending = op();
            async move { ok(pending.await) }
        });
    });
}

/// Group `mongo_get_pk` on `target`, after checking every variant finds
/// the seeded todo.
fn bench_get(c: &mut Criterion, rt: &Runtime, target: &Target, as_sent: &Document) -> Res<()> {
    let keys = Keys::default();
    let find = compiled_find(&compile_get(&target.db, &keys, LOOKUP_ID)?)?;
    let pipeline = former_pipeline();
    let todo = rt.block_on(orm_get(&target.db))?;
    let compiled = rt.block_on(driver_compiled(&target.todos, &find))?;
    let aggregated = rt.block_on(driver_aggregate(&target.todos, &pipeline))?;
    let found = rt.block_on(driver_find_one(&target.todos))?;
    rt.block_on(opmsg_find(&target.wire))?;
    let orm_shape = replayed_find(as_sent, false);
    let as_driver = replayed_find(as_sent, true);
    rt.block_on(opmsg_replay(&target.wire, &orm_shape))?;
    rt.block_on(opmsg_replay(&target.wire, &as_driver))?;
    let expected = todo_title(&found)?;
    if compiled != [found.clone()]
        || aggregated != compiled
        || todo.id != LOOKUP_ID
        || todo.title != expected
    {
        return Err(format!("variants disagree: {todo:?}, {compiled:?}, {found}").into());
    }
    if let Some(native) = &target.native {
        let natively = rt.block_on(orm_get(native))?;
        let same = |t: &Todo| (t.id, t.title.clone(), t.done);
        if same(&natively) != same(&todo) {
            let why = format!("native read disagrees: {natively:?}, {todo:?}");
            return Err(why.into());
        }
    }

    let mut group = c.benchmark_group("mongo_get_pk");
    bench_op(&mut group, rt, "orm", || orm_get(&target.db));
    if let Some(native) = &target.native {
        bench_op(&mut group, rt, "orm_native", || orm_get(native));
    }
    let (find, pipeline) = (&find, &pipeline);
    bench_op(&mut group, rt, "driver_compiled", || {
        driver_compiled(&target.todos, find)
    });
    bench_op(&mut group, rt, "driver_aggregate", || {
        driver_aggregate(&target.todos, pipeline)
    });
    bench_op(&mut group, rt, "driver_find_one", || {
        driver_find_one(&target.todos)
    });
    bench_op(&mut group, rt, "opmsg_find", || opmsg_find(&target.wire));
    let (orm_shape, as_driver) = (&orm_shape, &as_driver);
    bench_op(&mut group, rt, "opmsg_find_orm_shape", || {
        opmsg_replay(&target.wire, orm_shape)
    });
    bench_op(&mut group, rt, "opmsg_find_as_driver", || {
        opmsg_replay(&target.wire, as_driver)
    });
    group.bench_function("compile", |b| {
        b.iter(|| ok(compile_get(&target.db, &keys, black_box(LOOKUP_ID))));
    });
    group.finish();
    Ok(())
}

/// Group `mongo_list` on `target`, after checking the variants read the
/// same page.
fn bench_list(c: &mut Criterion, rt: &Runtime, target: &Target) -> Res<()> {
    let find = compiled_find(&compile_query(
        list_queryset(&target.db).plan(),
        &Keys::default(),
    )?)?;
    let todos = rt.block_on(orm_list(&target.db))?;
    let docs = rt.block_on(driver_compiled(&target.todos, &find))?;
    let fields = rt.block_on(driver_raw(&target.todos, &find))?;
    let ids: Vec<i64> = docs
        .iter()
        .map(|d| d.get_i64("_id"))
        .collect::<Result<_, _>>()?;
    // Newest first; the todos `create` added on start come before the seed.
    if todos.iter().map(|t| t.id).collect::<Vec<_>>() != ids
        || ids.len() != PAGE
        || !ids.is_sorted_by(|a, b| a > b)
        || fields != PAGE * 3
    {
        return Err(format!("variants disagree: {todos:?}, {ids:?}, {fields} fields").into());
    }
    if let Some(native) = &target.native {
        let natively = rt.block_on(orm_list(native))?;
        if natively.iter().map(|t| t.id).collect::<Vec<_>>() != ids {
            let why = format!("native page disagrees: {natively:?}, {ids:?}");
            return Err(why.into());
        }
    }

    let mut group = c.benchmark_group("mongo_list");
    bench_op(&mut group, rt, "orm", || orm_list(&target.db));
    if let Some(native) = &target.native {
        bench_op(&mut group, rt, "orm_native", || orm_list(native));
    }
    let find = &find;
    bench_op(&mut group, rt, "driver_compiled", || {
        driver_compiled(&target.todos, find)
    });
    bench_op(&mut group, rt, "driver_raw", || {
        driver_raw(&target.todos, find)
    });
    group.finish();
    Ok(())
}

/// The title of a todo document.
fn todo_title(todo: &Document) -> Res<&str> {
    Ok(todo.get_str("title")?)
}

/// Group `mongo_insert`: every variant on each target in turn.
fn bench_insert(c: &mut Criterion, rt: &Runtime, targets: [&Target; 2]) {
    let mut group = c.benchmark_group("mongo_insert");
    // Journaled writes take milliseconds; fewer, longer samples.
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(8));
    for target in targets {
        let id = |variant: &str| format!("{}/{variant}", target.label);
        bench_op(&mut group, rt, &id("orm"), || orm_create(&target.db));
        bench_op(&mut group, rt, &id("driver_counter_insert"), || {
            driver_counter_insert(target)
        });
        bench_op(&mut group, rt, &id("driver_insert_client_key"), || {
            driver_insert(target)
        });
        bench_op(&mut group, rt, &id("opmsg_insert"), || opmsg_insert(target));
    }
    group.finish();
}

fn main() {
    let Some(url) = std::env::var("MONGODB_URL").ok().filter(|u| !u.is_empty()) else {
        println!("mongo benchmarks skipped: set MONGODB_URL to run them");
        return;
    };
    let rt = ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build());
    let w1_url = with_param(&url, "w=1");
    let server_default = ok(rt.block_on(Target::open("w_default", &url, None, 1)));
    let w1 = ok(rt.block_on(Target::open("w1", &w1_url, Some(doc! { "w": 1_i32 }), 2)));
    ok(rt.block_on(seed(&server_default)));
    let as_sent = ok(rt.block_on(report_orm_commands("w_default", &url)));
    ok(rt.block_on(report_orm_commands("w1", &w1_url)));

    let mut c = Criterion::default().configure_from_args();
    ok(bench_get(&mut c, &rt, &server_default, &as_sent));
    ok(bench_list(&mut c, &rt, &server_default));
    bench_insert(&mut c, &rt, [&server_default, &w1]);
    c.final_summary();
}
