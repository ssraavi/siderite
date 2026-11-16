//! Generated keys: blocks reserved from `siderite_counters`.

use crate::common::Author;
use crate::fixtures::TestDb;
use siderite_orm::{BackendError, Db, OrmError};
use siderite_orm::{Model as _, ModelOps as _};
use std::collections::HashSet;

async fn save(db: &Db, name: &str) -> i64 {
    let mut a = Author::new(name, None);
    a.save(db).await.unwrap();
    a.id
}

async fn bulk(db: &Db, n: usize) -> Vec<i64> {
    Author::objects(db)
        .bulk_create(
            (0..n)
                .map(|i| Author::new(&format!("b{i}"), None))
                .collect(),
        )
        .await
        .unwrap()
        .iter()
        .map(|a| a.id)
        .collect()
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn separate_inserts_take_consecutive_keys_from_one_block() {
    let Some(t) = TestDb::open().await else {
        return;
    };
    let mut ids = Vec::new();
    for i in 0..5 {
        ids.push(save(&t.db, &format!("a{i}")).await);
    }
    assert_eq!(ids, [1, 2, 3, 4, 5]);
    // One `$inc` reserved the default block of 64.
    assert_eq!(t.counter("authors").await, Some(64));
    assert_eq!(bulk(&t.db, 3).await, [6, 7, 8]);
    assert_eq!(t.counter("authors").await, Some(64));
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn a_bulk_insert_larger_than_the_block_rest_gets_a_new_block() {
    let Some(t) = TestDb::with_key_block_size(8).await else {
        return;
    };
    let mut ids = Vec::new();
    for i in 0..5 {
        ids.push(save(&t.db, &format!("a{i}")).await);
    }
    assert_eq!(ids, [1, 2, 3, 4, 5]);
    assert_eq!(t.counter("authors").await, Some(8));
    // 3 keys are left; 10 rows reserve a block of 10 and skip 6..=8.
    assert_eq!(bulk(&t.db, 10).await, (9..=18).collect::<Vec<i64>>());
    assert_eq!(t.counter("authors").await, Some(18));
    // That block is used up: the next insert reserves 8 more.
    assert_eq!(save(&t.db, "next").await, 19);
    assert_eq!(t.counter("authors").await, Some(26));
    // A bulk insert that fits stays in the block.
    assert_eq!(bulk(&t.db, 7).await, (20..=26).collect::<Vec<i64>>());
    assert_eq!(t.counter("authors").await, Some(26));
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn block_size_one_advances_the_counter_per_insert() {
    let Some(t) = TestDb::with_key_block_size(1).await else {
        return;
    };
    for expected in 1..=3 {
        assert_eq!(save(&t.db, "one").await, expected);
        assert_eq!(t.counter("authors").await, Some(expected));
    }
    assert_eq!(bulk(&t.db, 4).await, [4, 5, 6, 7]);
    assert_eq!(t.counter("authors").await, Some(7));
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn blocks_continue_from_an_existing_counter() {
    let Some(t) = TestDb::open().await else {
        return;
    };
    // A counter written by another process (or the old one-per-insert
    // scheme) is continued, not reset.
    t.backend
        .database()
        .collection::<mongodb::bson::Document>("siderite_counters")
        .insert_one(mongodb::bson::doc! { "_id": "authors", "seq": 100_i64 })
        .await
        .unwrap();
    assert_eq!(save(&t.db, "a").await, 101);
    assert_eq!(t.counter("authors").await, Some(164));
    // A second backend on the same database reserves its own block.
    let other = Db::new(t.backend.clone().with_key_block_size(64));
    assert_eq!(save(&other, "b").await, 165);
    assert_eq!(save(&t.db, "c").await, 102);
    assert_eq!(t.counter("authors").await, Some(228));
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn clones_and_transactions_share_the_block() {
    let Some(t) = TestDb::open().await else {
        return;
    };
    assert_eq!(save(&t.db, "a").await, 1);
    let clone = Db::new(t.backend.clone());
    assert_eq!(save(&clone, "b").await, 2);
    let id =
        t.db.transaction(|tx| async move {
            let mut a = Author::new("in tx", None);
            a.save(&tx).await?;
            Ok::<_, OrmError>(a.id)
        })
        .await
        .unwrap();
    assert_eq!(id, 3);
    // A rolled-back insert leaves a gap; the key is not reused.
    let r: Result<(), OrmError> =
        t.db.transaction(|tx| async move {
            Author::new("rolled back", None).save(&tx).await?;
            Err(siderite_orm::QueryError::InvalidPlan("boom".into()).into())
        })
        .await;
    assert!(r.is_err());
    assert_eq!(save(&clone, "c").await, 5);
    assert_eq!(t.counter("authors").await, Some(64));
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn an_explicit_key_ahead_of_the_counter_is_still_a_constraint_error() {
    let Some(t) = TestDb::open().await else {
        return;
    };
    t.backend
        .database()
        .collection::<mongodb::bson::Document>("authors")
        .insert_one(mongodb::bson::doc! { "_id": 2_i64, "name": "explicit" })
        .await
        .unwrap();
    assert_eq!(save(&t.db, "a").await, 1);
    let err = Author::new("b", None).save(&t.db).await;
    assert!(
        matches!(err, Err(OrmError::Backend(BackendError::Constraint(_)))),
        "{err:?}"
    );
    // The failed key is spent; the next insert moves past it.
    assert_eq!(save(&t.db, "c").await, 3);
    t.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn concurrent_inserts_get_unique_keys() {
    let Some(t) = TestDb::with_key_block_size(16).await else {
        return;
    };
    // Create the collection first: a transaction that implicitly creates
    // it conflicts with concurrent writers.
    let first = save(&t.db, "first").await;
    let mut tasks = Vec::new();
    for i in 0..200_usize {
        let db = t.db.clone();
        tasks.push(tokio::spawn(async move {
            if i % 10 == 0 {
                // Bulk inserts of 5 interleave with single inserts.
                let ids = bulk(&db, 5).await;
                assert!(ids.windows(2).all(|w| w[1] == w[0] + 1), "{ids:?}");
                ids
            } else {
                vec![save(&db, &format!("c{i}")).await]
            }
        }));
    }
    let mut ids = vec![first];
    for task in tasks {
        ids.extend(task.await.unwrap());
    }
    assert_eq!(ids.len(), 1 + 180 + 20 * 5);
    let unique: HashSet<i64> = ids.iter().copied().collect();
    assert_eq!(unique.len(), ids.len());
    assert_eq!(Author::objects(&t.db).count().await.unwrap(), 281);
    let max = ids.iter().copied().max().unwrap();
    assert!(t.counter("authors").await.unwrap() >= max);
    t.cleanup().await;
}
