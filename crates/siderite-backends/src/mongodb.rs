//! MongoDB backend: compiles the supported subset of a
//! [`QueryPlan`](siderite_orm::QueryPlan) / [`WritePlan`](siderite_orm::WritePlan)
//! to `find` commands, filters, update documents and aggregation pipelines.
//!
//! * [`compile`] is a pure compiler (no server needed, unit-tested).
//! * [`MongoBackend`] executes compiled plans (MongoDB 5.0+; transactions
//!   need a replica set) and implements the same
//!   [`Backend`](siderite_orm::Backend) / [`Executor`](siderite_orm::Executor)
//!   traits as the SQL adapters, so it plugs into [`Db`](siderite_orm::Db).
//! * [`value`] maps [`Value`](siderite_orm::Value) to and from BSON.
//!   Read results are decoded from the raw BSON of the server's reply
//!   with that mapping: `Executor::fetch_each` hands each document to the
//!   model decoder in place, without building a `Document` or a `Row`.
//!
//! Anything the backend cannot express fails **before any I/O**, with a
//! capability error ([`BackendCapabilities::mongodb`](siderite_orm::BackendCapabilities::mongodb),
//! checked through `QueryPlan::check` / `WritePlan::check`) or
//! [`QueryError::InvalidPlan`](siderite_orm::QueryError::InvalidPlan).
//!
//! # Collections and keys
//!
//! A table is a collection of the same name. Plans carry no model metadata,
//! so the adapter needs to know which column is the primary key: it is the
//! column named `id` unless [`Keys`] says otherwise
//! (`MongoBackend::with_keys(Keys::default().with("tags", "slug"))`). That
//! column is stored as `_id` and nowhere else; filters, projections, sort
//! keys and update targets are translated, and `_id` is renamed back on
//! read. A key the plan does not supply (an insert that omits it) is
//! generated as an `i64` from a counter document in `siderite_counters`
//! (`{_id: <collection>, seq: <last reserved key>}`), like a SQL sequence
//! with `CACHE 64`: the backend reserves a block of keys with one `$inc` and
//! hands them out from memory, shared by its clones and transactions
//! ([`MongoBackend::with_key_block_size`] changes the size; `1` makes one
//! `$inc` per insert). A bulk insert gets consecutive keys in input order;
//! if the current block cannot hold all of them, a new block of at least the
//! batch size is reserved and the rest of the old one is skipped. The
//! counter lives outside the transaction, so a rollback leaves a gap, as do
//! the unused keys of a block when the process stops. Each backend (built
//! by `connect` / `from_client`, and so each process) reserves
//! its own blocks, so keys are unique but those of different processes
//! interleave and do not follow insertion order. Explicitly supplied keys do
//! not advance the counter (a generated key that collides with one fails
//! the insert with a constraint error). A collection whose key column is
//! not registered still works, but its key column is then an ordinary field
//! next to a generated `_id`, and duplicates of it are not rejected.
//!
//! # Mapping table
//!
//! ## `QueryPlan` fields
//!
//! A read is a `find` command when it can be one (see
//! [`compile::Read`]): the source is a table, there is no grouping,
//! `having`, aggregate or `DISTINCT` over projected columns, the projection
//! is empty or names plain columns of the collection under their own names,
//! the ordering names plain columns, and the limit is not 0. Every other
//! read is an aggregation pipeline. Both return the same rows; `find` is
//! cheaper on the server (an `_id` equality takes the point-lookup fast
//! path).
//!
//! | Field | `find` | Pipeline |
//! |---|---|---|
//! | `source` (table) | collection | collection |
//! | `projection` | inclusion projection (`{col: 1, ..}`, `_id: 1` for the key column, `_id: 0` otherwise); empty = whole documents | see below |
//! | `filter` | `filter`, the same document as the pipeline's `$match`, except that a filter that is exactly one `_id` equality with a scalar is `{_id: v}` (the form the server answers without parsing the query) | see below |
//! | `ordering` | `sort` (dropped for such an `_id` equality: at most one document matches) | see below |
//! | `limit` / `offset` | `limit` / `skip`; a limited result comes back whole in the first reply and the server closes the cursor (no `getMore` / `killCursors` unless the documents exceed 16 MiB): the first batch holds 101 documents by default, and a larger limit also sets `batchSize` to the limit (a smaller one sets none, which keeps `_id` lookups on the server's express path) | see below |
//!
//! Pipelines map every field:
//!
//! | Field | MongoDB |
//! |---|---|
//! | `source` (table) | collection |
//! | `source` (derived table, `from_subquery`) | inner pipeline followed by the outer stages (used by `count`, `exists`, `aggregate` on limited / distinct / grouped querysets) |
//! | `projection` | final `$project` (`_id: 0`, one computed field per output name); empty = whole documents with `_id` renamed to the key column |
//! | `joins` | capability error `Feature::Joins` (also `select_related` / `Related` columns) |
//! | `filter` | leading `$match`: query operators where a predicate has a query form, otherwise `{$expr: ..}` |
//! | `grouping` | `$group` with `_id: {k0: .., k1: ..}` (`_id: null` when only aggregates are projected) |
//! | `having` | `$match` with `$expr` after `$group` |
//! | `ordering` | `$sort` (plain columns sort directly, other expressions through a temporary `$addFields`; NULLs sort first ascending, as on SQLite) |
//! | `limit` / `offset` | `$limit` / `$skip` |
//! | `distinct: All` | `$group` on the projected fields, then `$replaceRoot`; ordering must then use projected expressions |
//! | `distinct: On` | capability error `Feature::DistinctOn` |
//! | `lock` | capability error `Feature::RowLocking` / `LockModifiers` |
//! | `compound` (set operations) | capability error `Feature::SetOperations` |
//!
//! A grouped plan needs an explicit projection, and every non-aggregate
//! expression in it, in `having` and in `ordering` must be a grouping key or
//! a projected alias (anything else is `InvalidPlan`). A single ungrouped
//! `COUNT(*)` compiles to `$count`. An ungrouped aggregate over no documents
//! still yields one row (`COUNT` is 0, the rest `NULL`), as in SQL.
//!
//! ## `WritePlan`
//!
//! | Plan | MongoDB |
//! |---|---|
//! | `Insert` | `insert_many` (ordered); the key column becomes `_id`; keys are generated when absent (see above); `returning` echoes the stored documents in input order |
//! | `Update` | `update_many`; literal assignments use `{$set: {..}}`, `F`-expression assignments use an update pipeline (`[{$set: {..}}]`, every expression sees the original document); `rows_affected` is the matched count; `returning` first pins the matching `_id`s, updates them, then reads them back by `_id`; the key column cannot be assigned |
//! | `Delete` | `delete_many`; `returning` reads the documents first |
//!
//! ## `Expr` nodes
//!
//! Predicates follow SQL three-valued logic: a row passes only when the
//! predicate is *true*. MongoDB's `$ne`, `$nin`, `$not` and `$nor` match
//! NULL and missing fields and `$expr` comparisons use BSON type order, so
//! negation is pushed down (De Morgan) with explicit non-NULL guards, and
//! `$expr` comparisons, `NOT`, `AND` and `OR` are compiled with a NULL
//! branch.
//!
//! | Node | Filter (query form) | Aggregation expression (`$expr`, projections, sorts, updates) |
//! |---|---|---|
//! | `Column` | field (`_id` for the key) | `"$field"`; boolean column as predicate: `{f: true}` |
//! | `Value` | operand | `{$literal: bson}`; `NULL` is `null` |
//! | `Binary` comparison | `{f: {$eq/$lt/$lte/$gt/$gte: v}}`; `<>` is `{$nin: [v, null]}`; `= NULL` is `{f: null}` | `$eq/$ne/$lt/$lte/$gt/$gte` guarded by `$cond` so a NULL operand gives NULL |
//! | `Binary` arithmetic | not a predicate | `$add`, `$subtract`, `$multiply`, `$divide`, `$mod` (`$divide` is always floating point, unlike SQL integer division) |
//! | `Unary` | `Not` pushes negation down; `Neg` n/a | `Not`: NULL-preserving `$not`; `Neg`: `$multiply: [x, -1]` |
//! | `And` / `Or` | `$and` / `$or`; empty `And` is `{}`, empty `Or` is `{$expr: false}` | Kleene logic with `$in: [false/true/null, [..]]` |
//! | `Lookup::IExact` | `{f: {$regex: "^escaped\\z", $options: "i"}}` | `$regexMatch` (text only, else NULL) |
//! | `Lookup::Contains` / `StartsWith` / `EndsWith` | `$regex` on the escaped needle, `$options: "i"` when case-insensitive, anchored with `^` / `\z` | `$regexMatch` |
//! | `Lookup::Regex` | `{f: {$regex: p}}` | `$regexMatch` |
//! | `Lookup::In` | `$in` (`$nin` with `null` when negated) | `$in` with a literal array |
//! | `Lookup::Range` | `{$gte, $lte}` (`$or` of `$lt` / `$gt` when negated) | `$and` of `$gte` / `$lte` |
//! | `Lookup::IsNull` | `{f: null}` / `{f: {$ne: null}}` | `$eq: [{$ifNull: [x, null]}, null]` |
//! | `Lookup::InSubquery`, `Exists`, `Subquery` | capability error `Feature::Subqueries` | same |
//! | `OuterRef` | `InvalidPlan` (only valid inside a subquery) | same |
//! | `Related` | capability error `Feature::Joins` | same |
//! | `Func::Lower` / `Upper` / `Length` / `Trim` | via `$expr` | `$toLower`, `$toUpper`, `$strLenCP`, `$trim` (NULL in, NULL out) |
//! | `Func::Coalesce` | via `$expr` | `$ifNull` |
//! | `Func::Concat` | via `$expr` | `$concat` with each argument wrapped in `$ifNull: [x, ""]` |
//! | `Func::Substr` | via `$expr` | `$substrCP` with `start - 1` (1-based), length defaults to the rest |
//! | `Func::Replace` | via `$expr` | `$replaceAll` |
//! | `Cast` | via `$expr` | `$convert` to `long` / `double` / `decimal` / `bool` / `string` / `date` (`SmallInt`, `Integer`, `BigInt`, `Real`, `Double`, `Decimal`, `Bool`, `Text`, `Timestamp`); other targets are `InvalidPlan` |
//! | `Case` | via `$expr` | `$switch` (default NULL) |
//! | `DatePart` | via `$expr` | `$year`, `$month`, `$dayOfMonth`, `$isoWeek`, quarter from `$month`, `$hour`, `$minute`, `$second`, `$dateToString` (`%Y-%m-%d`) for `Date`. Dates and times are stored as text, so a string operand is parsed with `$dateFromString` (dates) or sliced (times) |
//! | `Aggregate::Count(*)` | | `{$sum: 1}` (`$count` alone) |
//! | `Aggregate::Count(x)` | | `$sum` of 1 per non-NULL value; `DISTINCT` via `$addToSet` and `$size` |
//! | `Aggregate::Sum` | | `$sum` plus a count of numeric values so an all-NULL group is NULL, not 0 |
//! | `Aggregate::Avg` / `Min` / `Max` | | `$avg` / `$min` / `$max` |
//! | `Aggregate::StdDev` / `Variance` | | `$stdDevSamp` / `$stdDevPop`; variance is `$pow` of the deviation by 2 (statistical aggregates are supported) |
//! | `Aggregate::StringAgg` | | `$push` (or `$addToSet`) then `$reduce` with `$concat` (element order is unspecified) |
//! | `Aggregate::ArrayAgg` | | capability error `Feature::Arrays` |
//! | `Aggregate` `filter` | | the argument becomes `{$cond: [filter, x, null]}`, which every accumulator ignores |
//! | `Window` | capability error `Feature::WindowFunctions` | same |
//!
//! # Values
//!
//! | `Value` | BSON |
//! |---|---|
//! | `Null` | null |
//! | `Bool` | boolean |
//! | `Int` | int64 (int32 and int64 both decode) |
//! | `Float` | double |
//! | `Decimal` | Decimal128 (exact, sums and averages stay decimal) |
//! | `Text` | string |
//! | `Bytes` | binary, generic subtype |
//! | `Uuid` | binary subtype 4 (16 bytes) |
//! | `Timestamp` | BSON datetime, **millisecond precision**: microseconds are truncated on write, so a value read back can differ from the one written; `insert`/`update` `returning` rows reflect what was stored |
//! | `Date`, `Time` | canonical text as on SQLite (`%Y-%m-%d`, `%H:%M:%S%.6f`), which sorts and compares correctly |
//! | `Json` | objects and arrays are embedded documents / arrays (numbers become int64 or double; integers above `i64::MAX` are rejected); JSON scalars are stored as their JSON text (`5`, `"x"`) so they decode back unchanged |
//!
//! Decoding is by BSON type: documents and arrays become `Value::Json`,
//! ObjectIds become hex text, other unsupported types are decode errors.
//!
//! # Transactions
//!
//! `begin` starts a session and a transaction (`TransactionSupport::Flat`).
//! There are no savepoints, so a nested `Db::transaction` (and therefore
//! `bulk_create` on a transactional handle) is a capability error, and no
//! isolation level can be requested. Dropping a transaction aborts it.
//! MongoDB has no foreign keys: `ON DELETE` actions do nothing, and unique
//! constraints exist only where [`MongoBackend::create_unique_index`] made
//! one (plus `_id`). Duplicate-key and document-validation failures map to
//! [`BackendError::Constraint`](siderite_orm::BackendError::Constraint).
//!
//! # Raw access
//!
//! `raw_sql`, `raw_execute` and `execute_script` are capability errors
//! (`Feature::RawSql`); [`MongoBackend::raw_command`] runs a database
//! command instead.
//!
//! # Native data path
//!
//! With the `mongodb-native` feature (off by default),
//! `MongoBackend::connect_native` (or `connect_native_with` and
//! `NativeOptions`) also opens a pool of the backend's own `OP_MSG`
//! connections and sends reads outside transactions there, skipping the
//! driver's per-operation server selection, implicit session checkout and
//! hand-off to its pool worker. The driver `Client` stays the source of
//! truth: it is connected as by `connect` and runs every write,
//! transaction, admin command (`raw_command`) and index creation.
//!
//! **Eligibility** is decided once, at connect: a plain `mongodb://` URL
//! (not `mongodb+srv://`) without TLS, credentials, `loadBalanced` or a
//! declared server API, with the primary read preference (the default),
//! and a writable primary reachable from the seed list. The primary is
//! found with `hello` on native connections (following each member's
//! `primary` and `hosts`, except with `directConnection=true`, which allows
//! only the seed itself); it must report `maxWireVersion` 13 or later
//! (MongoDB 5.0) and, when the URL names a `replicaSet`, that `setName`. A
//! standalone server or `mongos` that reports itself writable qualifies
//! when the URL names no replica set. Otherwise the backend silently uses
//! the driver for everything; `MongoBackend::data_path` says which path is
//! active and `MongoBackend::native_ineligibility` why native was not.
//! SRV, TLS and authentication are planned follow-ups.
//!
//! **Supported operations**: `Read::Find` and `Read::Pipeline` (the
//! `find` / `aggregate` commands of [`compile::Read`]) run by `fetch`,
//! `fetch_each` and the reads inside `update` / `delete` with `RETURNING`,
//! whenever no transaction (session) is involved; transactions always use
//! the driver. Commands are written straight into a reused buffer (the
//! compiled filter, sort, projection and stages serialized in place) with
//! the fields the driver sends (`limit`, `skip`, `batchSize` as the driver
//! computes it, the URL's `readConcern`, `$db`), but without `lsid` and
//! `$clusterTime`: a read outside a session needs no session id (the
//! server opens no session for it, and the cursor is killed or exhausted
//! by this backend), and causal consistency is not offered outside
//! sessions on either path. When the first batch leaves a cursor open,
//! `getMore` runs on the same connection until it is exhausted; a read
//! that stops early (the row sink fails) sends `killCursors`. Replies are
//! decoded by the same raw-BSON decoder as the driver path, so rows are
//! identical.
//!
//! **Pool**: `maxPoolSize` connections (default 10, like the driver; in
//! addition to the driver's own pool), opened on demand with
//! `connectTimeoutMS` and kept with `TCP_NODELAY`. A checkout takes a
//! semaphore permit and pops an idle connection in the calling task (no
//! channel hop); it waits at most `waitQueueTimeoutMS` (default 30 s) and
//! then fails with `BackendError::Connection`. Each connection reuses its
//! request and reply buffers. Cancellation is safe: a read whose future is
//! dropped mid-write closes its connection, and one dropped while its reply
//! is due hands the connection to a task that reads the reply to the end,
//! kills any cursor it opened and only then returns the connection.
//!
//! **Errors and fallback**: an `ok: 0` reply is classified exactly as the
//! driver path classifies the same server error (duplicate keys and
//! validation are `Constraint`, the rest `Database`). A network error, or
//! a "not primary" / shutdown / interrupted reply (codes 10107, 13435,
//! 13436, 189, 91, 11600, 11602 and the host / network codes 6, 7, 89,
//! 9001), closes the connection, retires every connection to that server
//! and makes the next checkout look for the primary again; the read is
//! then retried once through the driver, which selects the new primary
//! itself (only if no row was delivered yet; reads are idempotent, and no
//! write runs natively). While no primary is known, reads use the driver.
//!
//! **Limitations**: no SRV, TLS, authentication, compression or server API
//! versioning; reads only; no idle-connection reaping (`maxIdleTimeMS`), no
//! background monitoring (a primary change is noticed through errors); the
//! two pools together may hold up to twice `maxPoolSize` connections.

pub mod compile;
#[cfg(test)]
mod compile_tests;
mod decode;
mod exec;
mod keys;
#[cfg(feature = "mongodb-native")]
mod native;
pub mod value;

pub use compile::Keys;
pub use exec::MongoBackend;
#[cfg(feature = "mongodb-native")]
pub use native::{DataPath, NativeOptions};
