//! Rows handed over by `fetch_each`, decoded from the raw BSON of the
//! reply, match what `fetch` returns and how `from_bson` maps values.

use crate::fixtures::TestDb;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{Binary, Bson, DateTime, Decimal128, Document, Regex, doc};
use siderite_backends::mongodb::value::from_bson;
use siderite_orm::{
    Backend as _, Cells, Count, DistinctMode, Executor, Expr, InsertPlan, OrderDirection, OrmError,
    QueryError, QueryPlan, Row, RowSink, Sum, Value, WritePlan,
};
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

/// Collects what `fetch_each` hands over: whole rows, or the cells at
/// `take` (`None` where the row has no such column), and each row's header.
#[derive(Default)]
struct Probe {
    take: Vec<usize>,
    fail_at: Option<usize>,
    rows: Vec<Row>,
    taken: Vec<Vec<Option<Value>>>,
    headers: Vec<Arc<[String]>>,
}

impl RowSink for Probe {
    fn push(&mut self, cells: &mut dyn Cells) -> Result<(), QueryError> {
        if self.fail_at == Some(self.headers.len()) {
            return Err(QueryError::Model("rejected".into()));
        }
        self.headers.push(Arc::clone(cells.columns()));
        if self.take.is_empty() {
            self.rows.push(cells.to_row()?);
        } else {
            let taken = self.take.iter().map(|&index| {
                let mut value = Value::Null;
                Ok(cells.take(index, &mut value)?.then_some(value))
            });
            self.taken.push(taken.collect::<Result<_, QueryError>>()?);
        }
        Ok(())
    }
}

/// A document with a field of every BSON type that maps to a value.
fn every_type() -> Document {
    doc! {
        "_id": 1_i64,
        "name": "a",
        "small": 5_i32,
        "big": 1_i64 << 40,
        "ratio": 1.5,
        "flag": true,
        "price": Decimal128::from_str("12.50").unwrap(),
        "uid": Binary { subtype: BinarySubtype::Uuid, bytes: Uuid::from_u128(7).as_bytes().to_vec() },
        "blob": Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2, 3] },
        "short_uuid": Binary { subtype: BinarySubtype::Uuid, bytes: vec![9] },
        "at": DateTime::from_millis(1_700_000_000_123),
        "meta": { "a": [1_i32, 2.5, Bson::Null], "$k": { "b": true } },
        "list": [1_i64, "x"],
        "oid": ObjectId::parse_str("65f000000000000000000001").unwrap(),
        "nothing": Bson::Null,
        "undefined": Bson::Undefined,
    }
}

/// `doc` as `from_bson` maps it, `_id` named `id`.
fn mapped(doc: &Document) -> Vec<(String, Value)> {
    doc.iter()
        .map(|(key, value)| {
            let name = if key == "_id" { "id" } else { key.as_str() };
            (name.to_owned(), from_bson(value.clone(), name).unwrap())
        })
        .collect()
}

async fn seeded() -> Option<TestDb> {
    let t = TestDb::open().await?;
    let things = t.backend.database().collection::<Document>("things");
    things
        .insert_many([
            every_type(),
            doc! { "_id": 2_i64, "name": "b" },
            // A stored `id` field wins over `_id` in whole documents.
            doc! { "_id": 3_i64, "name": "c", "id": 99_i64 },
        ])
        .await
        .unwrap();
    Some(t)
}

fn things() -> QueryPlan {
    QueryPlan::from_table("things").order_by(Expr::col("id"), OrderDirection::Asc)
}

async fn each(db: &dyn Executor, plan: &QueryPlan, take: &[usize]) -> Probe {
    let mut probe = Probe {
        take: take.to_vec(),
        ..Probe::default()
    };
    db.fetch_each(plan, &mut probe).await.unwrap();
    probe
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn whole_documents_decode_from_raw_bson() {
    let Some(t) = seeded().await else {
        return;
    };
    let db = &t.backend;
    let probe = each(db, &things(), &[]).await;
    assert_eq!(probe.rows, db.fetch(&things()).await.unwrap().rows);
    let rows: Vec<Vec<(String, Value)>> = probe.rows.into_iter().map(Row::into_columns).collect();
    assert_eq!(rows[0], mapped(&every_type()));
    let row = &rows[0];
    let value = |name: &str| &row.iter().find(|(n, _)| n == name).unwrap().1;
    assert_eq!(value("small"), &Value::Int(5));
    assert_eq!(value("price"), &Value::Decimal("12.50".parse().unwrap()));
    assert_eq!(value("uid"), &Value::Uuid(Uuid::from_u128(7)));
    assert_eq!(value("short_uuid"), &Value::Bytes(vec![9]));
    assert_eq!(
        value("oid"),
        &Value::Text("65f000000000000000000001".into())
    );
    assert_eq!(value("undefined"), &Value::Null);
    assert_eq!(
        value("meta"),
        &Value::Json(serde_json::json!({"a": [1, 2.5, null], "$k": {"b": true}}))
    );
    assert!(
        matches!(value("at"), Value::Timestamp(at) if at.timestamp_millis() == 1_700_000_000_123)
    );
    assert_eq!(
        rows[1],
        [
            ("id".into(), Value::Int(2)),
            ("name".into(), Value::Text("b".into()))
        ]
    );
    assert_eq!(
        rows[2],
        [
            ("name".into(), Value::Text("c".into())),
            ("id".into(), Value::Int(99))
        ]
    );
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn projected_cells_share_a_header_and_missing_fields_are_null() {
    let Some(t) = seeded().await else {
        return;
    };
    let db = &t.backend;
    let plan = things()
        .select(Expr::col("price"), None)
        .select(Expr::col("id"), None)
        .select(Expr::col("uid"), None)
        .select(Expr::col("at"), None);
    let fetched = db.fetch(&plan).await.unwrap().rows;
    let whole = each(db, &plan, &[]).await;
    assert_eq!(whole.rows, fetched);
    assert!(whole.headers.windows(2).all(|w| Arc::ptr_eq(&w[0], &w[1])));
    assert_eq!(&*whole.headers[0], ["price", "id", "uid", "at"]);

    let cells = each(db, &plan, &[1, 0, 4]).await;
    let ids = [1, 2, 3].map(Value::Int);
    let expected: Vec<_> = fetched
        .iter()
        .zip(ids)
        .map(|(row, id)| vec![Some(id), row.get("price").cloned(), None])
        .collect();
    assert_eq!(cells.taken, expected);
    assert_eq!(cells.taken[1][1], Some(Value::Null), "missing field");
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn pipelines_and_empty_aggregates_hand_over_rows() {
    let Some(t) = seeded().await else {
        return;
    };
    let db = &t.backend;
    let distinct = QueryPlan::from_table("things")
        .select(Expr::col("name"), Some("label"))
        .distinct(DistinctMode::All)
        .order_by(Expr::col("label"), OrderDirection::Asc);
    let probe = each(db, &distinct, &[]).await;
    assert_eq!(probe.rows, db.fetch(&distinct).await.unwrap().rows);
    assert_eq!(probe.rows.len(), 3);
    assert_eq!(probe.rows[0].get("label"), Some(&Value::Text("a".into())));

    let none = QueryPlan::from_table("things").filter(Expr::col("name").eq("zzz"));
    let count = none.clone().select(Count::all(), Some("n"));
    let probe = each(db, &count, &[]).await;
    assert_eq!(probe.rows, db.fetch(&count).await.unwrap().rows);
    assert_eq!(
        probe.rows[0].clone().into_columns(),
        [("n".into(), Value::Int(0))]
    );
    let sum = none.select(Sum::of(Expr::col("small")), Some("s"));
    let probe = each(db, &sum, &[0]).await;
    assert_eq!(probe.taken, [[Some(Value::Null)]]);
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn fetch_each_reports_sink_and_decode_errors() {
    let Some(t) = seeded().await else {
        return;
    };
    let db = &t.backend;
    let mut rejecting = Probe {
        fail_at: Some(1),
        ..Probe::default()
    };
    let rejected = db.fetch_each(&things(), &mut rejecting).await;
    assert!(
        matches!(&rejected, Err(OrmError::Query(QueryError::Model(m))) if m == "rejected"),
        "{rejected:?}"
    );
    assert_eq!(rejecting.rows.len(), 1);

    t.backend
        .database()
        .collection::<Document>("things")
        .insert_one(
            doc! { "_id": 4_i64, "re": Regex { pattern: "a".into(), options: String::new() } },
        )
        .await
        .unwrap();
    let projected = things().select(Expr::col("re"), None);
    for plan in [projected, things()] {
        let mut probe = Probe::default();
        let failed = db.fetch_each(&plan, &mut probe).await;
        assert!(
            matches!(&failed, Err(OrmError::Query(QueryError::Decode { column, .. })) if column == "re"),
            "{failed:?}"
        );
        assert!(db.fetch(&plan).await.is_err());
    }
    t.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit MONGODB_URL and an isolated live service"]
async fn fetch_each_in_a_transaction_sees_its_writes() {
    let Some(t) = seeded().await else {
        return;
    };
    let tx = t.backend.begin(None).await.unwrap();
    let insert = InsertPlan {
        table: "things".into(),
        columns: vec!["id".into(), "name".into()],
        rows: vec![vec![Value::Int(5), Value::Text("e".into())]],
        returning: Vec::new(),
    };
    tx.execute(&WritePlan::Insert(insert)).await.unwrap();
    let plan = things().select(Expr::col("name"), None);
    let probe = each(&*tx, &plan, &[]).await;
    assert_eq!(probe.rows, tx.fetch(&plan).await.unwrap().rows);
    assert_eq!(probe.rows.len(), 4);
    assert_eq!(probe.rows[3].get("name"), Some(&Value::Text("e".into())));
    tx.rollback().await.unwrap();
    assert_eq!(each(&t.backend, &plan, &[]).await.rows.len(), 3);
    t.cleanup().await;
}
