//! Simple reads run as `find` commands; the rest stay pipelines.

use crate::common::{Author, Book, seed, titles};
use crate::fixtures::TestDb;
use mongodb::Client;
use mongodb::bson::{Bson, Document};
use mongodb::event::EventHandler;
use mongodb::event::command::CommandEvent;
use mongodb::options::ClientOptions;
use siderite_backends::mongodb::{Keys, MongoBackend};
use siderite_orm::{Count, Db, OrmError, QueryError};
use siderite_orm::{Model as _, ModelOps as _};
use std::sync::{Arc, Mutex};

/// Commands that read or page through results.
const READS: [&str; 4] = ["find", "aggregate", "getMore", "killCursors"];

/// A handle on `t`'s database whose read commands are recorded.
struct Watched {
    db: Db,
    seen: Arc<Mutex<Vec<Document>>>,
}

impl Watched {
    async fn on(t: &TestDb) -> Self {
        let url = std::env::var("MONGODB_URL").unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let mut options = ClientOptions::parse(&url).await.unwrap();
        options.command_event_handler = Some(EventHandler::callback(move |event| {
            if let CommandEvent::Started(started) = event
                && READS.contains(&started.command_name.as_str())
            {
                sink.lock().unwrap().push(started.command);
            }
        }));
        let backend = MongoBackend::from_client(
            Client::with_options(options).unwrap(),
            t.backend.database().name(),
        )
        .with_keys(Keys::default().with("tags", "slug"));
        Self {
            db: Db::new(backend),
            seen,
        }
    }

    /// The names of the commands recorded since the last call.
    fn take(&self) -> Vec<String> {
        self.commands()
            .iter()
            .map(|c| c.keys().next().unwrap().clone())
            .collect()
    }

    /// The commands recorded since the last call.
    fn commands(&self) -> Vec<Document> {
        std::mem::take(&mut *self.seen.lock().unwrap())
    }
}

async fn authors(db: &Db, n: usize) {
    Author::objects(db)
        .bulk_create(
            (0..n)
                .map(|i| Author::new(&format!("a{i:03}"), Some(i32::try_from(i % 7).unwrap())))
                .collect(),
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn pk_get_and_paged_filters_are_finds() {
    let Some((t, s)) = TestDb::seeded().await else {
        return;
    };
    let w = Watched::on(&t).await;
    let db = &w.db;
    let rust = s.book("Rust");
    let got = Book::objects(db).get(Book::id.eq(rust.id)).await.unwrap();
    assert_eq!(&got, rust);
    let get = w.commands();
    assert_eq!(get.len(), 1, "{get:?}");
    assert!(get[0].contains_key("find"), "{get:?}");
    assert!(!get[0].contains_key("sort"), "{get:?}");
    assert_eq!(get[0].get("limit"), Some(&Bson::Int64(2)));
    // No batch size: it would keep the server off its `_id` express path.
    assert!(!get[0].contains_key("batchSize"), "{get:?}");

    // An ordering next to a key equality is dropped; the row is the same.
    let by_key = Book::objects(db)
        .filter(Book::id.eq(rust.id))
        .order_by([Book::likes.desc()])
        .all()
        .await
        .unwrap();
    assert_eq!(by_key, std::slice::from_ref(rust));
    let cmd = w.commands();
    assert!(cmd[0].contains_key("find") && !cmd[0].contains_key("sort"));

    // Filter + order + offset + limit, including `$expr` filters.
    let page = Book::objects(db)
        .filter(Book::likes.gt(2))
        .order_by([Book::likes.desc()])
        .offset(1)
        .limit(2)
        .all()
        .await
        .unwrap();
    assert_eq!(titles(&page), ["SQL", "Async"]);
    let page = Book::objects(db)
        .filter(Book::likes.gt(Book::dislikes))
        .order_by([Book::title.asc()])
        .limit(10)
        .all()
        .await
        .unwrap();
    assert_eq!(titles(&page), ["Go", "Rust", "SQL"]);
    // NULLs sort first ascending, as through the pipeline.
    let by_pages = Book::objects(db)
        .order_by([Book::pages.asc(), Book::id.asc()])
        .all()
        .await
        .unwrap();
    assert_eq!(titles(&by_pages), ["Async", "Zig", "SQL", "Go", "Rust"]);
    assert_eq!(w.take(), ["find", "find", "find"]);

    // Grouped and computed reads stay pipelines.
    assert_eq!(Book::objects(db).count().await.unwrap(), 5);
    let n = Book::objects(db)
        .aggregate([("n", Count::all())])
        .await
        .unwrap();
    assert_eq!(n.get_as::<i64>("n").unwrap(), 5);
    assert_eq!(w.take(), ["aggregate", "aggregate"]);
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn projections_of_plain_columns_are_finds() {
    let Some((t, s)) = TestDb::seeded().await else {
        return;
    };
    let w = Watched::on(&t).await;
    let db = &w.db;
    let rows = Book::objects(db)
        .filter(Book::author.eq(s.ann.id))
        .order_by([Book::title.asc()])
        .values(["title", "likes"])
        .await
        .unwrap();
    let got: Vec<(String, i64)> = rows
        .iter()
        .map(|r| (r.get_as("title").unwrap(), r.get_as("likes").unwrap()))
        .collect();
    assert_eq!(got, [("Async".into(), 5), ("Rust".into(), 10)]);
    assert_eq!(rows[0].len(), 2);
    let ids: Vec<i64> = Book::objects(db)
        .order_by([Book::id.desc()])
        .values_list(["id"])
        .await
        .unwrap();
    let mut expected: Vec<i64> = s.books.iter().map(|b| b.id).collect();
    expected.reverse();
    assert_eq!(ids, expected);
    let pairs: Vec<(i64, Option<i32>)> = Book::objects(db)
        .filter(Book::pages.is_null())
        .values_list([Book::id.select(), Book::pages.select()])
        .await
        .unwrap();
    assert_eq!(pairs, [(s.book("Async").id, None)]);
    let cmds = w.commands();
    assert_eq!(cmds.len(), 3, "{cmds:?}");
    assert!(cmds.iter().all(|c| c.contains_key("find")), "{cmds:?}");
    // A renamed column is a pipeline `$project`.
    let renamed = Book::objects(db)
        .filter(Book::id.eq(s.book("Go").id))
        .values([("name", Book::title)])
        .await
        .unwrap();
    assert_eq!(renamed[0].get_as::<String>("name").unwrap(), "Go");
    assert_eq!(w.take(), ["aggregate"]);
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn large_limits_return_every_row_in_one_batch() {
    let Some(t) = TestDb::open().await else {
        return;
    };
    authors(&t.db, 260).await;
    let w = Watched::on(&t).await;
    let db = &w.db;
    // More than the default first batch of 101, in one reply.
    let limited = Author::objects(db)
        .order_by([Author::id.asc()])
        .limit(150)
        .all()
        .await
        .unwrap();
    assert_eq!(limited.len(), 150);
    assert_eq!(limited[0].name, "a000");
    assert_eq!(limited[149].name, "a149");
    let cmds = w.commands();
    assert_eq!(cmds.len(), 1, "{cmds:?}");
    assert_eq!(cmds[0].get("batchSize"), Some(&Bson::Int32(151)));
    // A limit above the row count.
    let all = Author::objects(db)
        .filter(Author::age.ge(0))
        .order_by([Author::name.desc()])
        .offset(10)
        .limit(1000)
        .all()
        .await
        .unwrap();
    assert_eq!(all.len(), 250);
    assert_eq!(all[0].name, "a249");
    assert_eq!(w.take(), ["find"]);
    // No limit: the cursor is drained with getMore.
    assert_eq!(Author::objects(db).all().await.unwrap().len(), 260);
    assert_eq!(w.take(), ["find", "getMore"]);
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn finds_inside_a_transaction_see_its_writes() {
    let Some(t) = TestDb::open().await else {
        return;
    };
    authors(&t.db, 120).await;
    let w = Watched::on(&t).await;
    let db = &w.db;
    db.transaction(|tx| async move {
        let mut new = Author::new("zz-new", Some(99));
        new.save(&tx).await?;
        let got = Author::objects(&tx).get(Author::id.eq(new.id)).await?;
        assert_eq!(got.name, "zz-new");
        let top = Author::objects(&tx)
            .order_by([Author::name.desc()])
            .limit(2)
            .all()
            .await?;
        assert_eq!(top[0].name, "zz-new");
        assert_eq!(top[1].name, "a119");
        // Every row, past the first batch, on the session.
        assert_eq!(Author::objects(&tx).all().await?.len(), 121);
        let ages: Vec<Option<i32>> = Author::objects(&tx)
            .filter(Author::id.eq(new.id))
            .values_list(["age"])
            .await?;
        assert_eq!(ages, [Some(99)]);
        Ok::<_, OrmError>(())
    })
    .await
    .unwrap();
    let cmds = w.commands();
    assert!(!cmds.is_empty());
    assert!(
        cmds.iter()
            .all(|c| !c.contains_key("aggregate") && c.contains_key("txnNumber")),
        "{cmds:?}"
    );
    assert_eq!(Author::objects(&t.db).count().await.unwrap(), 121);
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn empty_finds() {
    let Some(t) = TestDb::open().await else {
        return;
    };
    let w = Watched::on(&t).await;
    let db = &w.db;
    // A collection that does not exist yet.
    assert!(Author::objects(db).all().await.unwrap().is_empty());
    let missing = Author::objects(db).get(Author::id.eq(1)).await;
    assert!(
        matches!(missing, Err(OrmError::Query(QueryError::DoesNotExist))),
        "{missing:?}"
    );
    seed(&t.db).await;
    let none = Book::objects(db)
        .filter(Book::likes.gt(100))
        .order_by([Book::likes.desc()])
        .limit(5)
        .all()
        .await
        .unwrap();
    assert!(none.is_empty());
    assert!(
        Book::objects(db)
            .offset(10)
            .values(["title"])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(w.take(), ["find", "find", "find", "find"]);
    t.cleanup().await;
}
