//! MongoDB: a primary-key read and a single-document insert through the
//! ORM, through the raw driver, and as a hand-rolled `OP_MSG` round trip
//! (the floor), plus the compile-only cost of the read.
//!
//! Needs a live server: set `MONGODB_URL` (for example
//! `mongodb://127.0.0.1:27017/?replicaSet=rs0`). Without it the binary
//! prints a note and runs nothing. Every variant works in the
//! `siderite_bench` database, which is dropped and seeded with 100 todos
//! (and their key counter, as the benchmark runner seeds them) on start.
//! Before measuring, the commands the ORM sends for one `get` and one
//! `create` are printed, and the read's compiled pipeline is checked
//! against them.
//!
//! * `mongo_get_pk`: `orm` is `Todo::objects(&db).get(Todo::id.eq(1))`;
//!   `driver_pipeline` runs the pipeline the ORM compiles; `driver_find_one`
//!   is `find_one({_id: 1})`; `opmsg_find` is a `find` with `limit: 1` and
//!   `singleBatch` on one raw connection; `compile` builds the queryset and
//!   compiles it, without I/O.
//! * `mongo_insert`: `orm` is `create`; `driver_counter_insert` reserves the
//!   key from the counter (`findAndModify`) and then inserts, as the ORM
//!   does; `driver_insert_client_key` and `opmsg_insert` insert with a key
//!   chosen by the client. Ids starting `w_default/` leave the write
//!   concern to the server (majority, journaled, on a default replica set);
//!   `w1/` ids set `w=1` on the URL, or in the raw command.
//!
//! All variants run one request at a time on a current-thread Tokio
//! runtime, so each time is the latency of one request.

#[path = "common/opmsg.rs"]
mod opmsg;

use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, Criterion};
use mongodb::bson::{Bson, Document, doc};
use mongodb::event::EventHandler;
use mongodb::event::command::CommandEvent;
use mongodb::options::{ClientOptions, ReturnDocument};
use mongodb::{Client, Collection};
use siderite::orm::Db;
use siderite::prelude::*;
use siderite_backends::mongodb::compile::{CompiledQuery, compile_query};
use siderite_backends::mongodb::{Keys, MongoBackend};
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
        Ok(Self {
            label,
            db: Db::new(MongoBackend::from_client(client, DATABASE)),
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
/// fields) and check the read's pipeline is the one [`compile_get`]
/// builds, so `driver_pipeline` and `compile` time the same query.
async fn report_orm_commands(label: &str, url: &str) -> Res<()> {
    let (db, commands) = observe_orm(url).await?;
    let mut compiled = None;
    for mut command in commands {
        command.remove("lsid");
        command.remove("$clusterTime");
        println!("ORM command ({label}): {command}");
        if command.contains_key("aggregate") {
            compiled = command.remove("pipeline");
        }
    }
    let ours = compile_get(&db, &Keys::default(), LOOKUP_ID)?.pipeline;
    let ours = Bson::Array(ours.into_iter().map(Bson::Document).collect());
    if compiled.as_ref() != Some(&ours) {
        return Err(format!("ORM sent {compiled:?}, bench compiles {ours}").into());
    }
    Ok(())
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

/// Variant `driver_pipeline`: `pipeline` through `aggregate`, every
/// document collected as the ORM collects them.
async fn driver_pipeline(
    todos: &Collection<Document>,
    pipeline: &[Document],
) -> Res<Vec<Document>> {
    let mut cursor = todos.aggregate(pipeline.to_vec()).await?;
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
    wire.run(&command, |reply| {
        let batch = reply.get_document("cursor")?.get_array("firstBatch")?;
        match batch.get_document(0)?.get_i64("_id")? {
            LOOKUP_ID => Ok(()),
            _ => Err("find returned another todo".into()),
        }
    })
    .await
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

/// Variant `driver_counter_insert`: reserve one key as the ORM does, then
/// insert.
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
fn bench_get(c: &mut Criterion, rt: &Runtime, target: &Target) -> Res<()> {
    let keys = Keys::default();
    let pipeline = compile_get(&target.db, &keys, LOOKUP_ID)?.pipeline;
    let todo = rt.block_on(orm_get(&target.db))?;
    let raw = rt.block_on(driver_pipeline(&target.todos, &pipeline))?;
    let found = rt.block_on(driver_find_one(&target.todos))?;
    rt.block_on(opmsg_find(&target.wire))?;
    let expected = todo_title(&found)?;
    if raw != [found.clone()] || todo.id != LOOKUP_ID || todo.title != expected {
        return Err(format!("variants disagree: {todo:?}, {raw:?}, {found}").into());
    }

    let mut group = c.benchmark_group("mongo_get_pk");
    bench_op(&mut group, rt, "orm", || orm_get(&target.db));
    let pipeline = &pipeline;
    bench_op(&mut group, rt, "driver_pipeline", || {
        driver_pipeline(&target.todos, pipeline)
    });
    bench_op(&mut group, rt, "driver_find_one", || {
        driver_find_one(&target.todos)
    });
    bench_op(&mut group, rt, "opmsg_find", || opmsg_find(&target.wire));
    group.bench_function("compile", |b| {
        b.iter(|| ok(compile_get(&target.db, &keys, black_box(LOOKUP_ID))));
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
    ok(rt.block_on(report_orm_commands("w_default", &url)));
    ok(rt.block_on(report_orm_commands("w1", &w1_url)));

    let mut c = Criterion::default().configure_from_args();
    ok(bench_get(&mut c, &rt, &server_default));
    bench_insert(&mut c, &rt, [&server_default, &w1]);
    c.final_summary();
}
