//! The MongoDB executor: [`MongoBackend`] and its transactions.

use super::compile::{
    CompiledDelete, CompiledInsert, CompiledQuery, CompiledUpdate, CompiledWrite, FindQuery, Keys,
    Read, UpdateSpec, compile_query, compile_write,
};
use super::decode::{Collect, Decoder, doc_to_row, raw_id};
use super::keys::KeyAllocator;
use ::mongodb::bson::raw::RawDocument;
use ::mongodb::bson::{Bson, Document, doc};
use ::mongodb::error::{Error as DriverError, ErrorKind};
use ::mongodb::{Client, ClientSession, Collection, Database};
use async_trait::async_trait;
use siderite_orm::{
    Backend, BackendCapabilities, BackendCapabilityError, BackendError, BackendKind, ExecResult,
    Executor, Feature, IsolationLevel, OrmError, QueryError, QueryPlan, QueryResult, RowSink,
    Transaction, Value, WritePlan,
};
use std::sync::Arc;
use tokio::sync::Mutex;

/// MongoDB adapter: compiles plans to `find` commands, filters and
/// pipelines and runs them.
///
/// Collections are named after tables. The primary-key column of a
/// collection is stored as `_id` (see [`Keys`]); rows come back with `_id`
/// renamed to that column. Integer keys that a plan does not supply are
/// generated from a counter collection, `siderite_counters`, in blocks (see
/// [`MongoBackend::with_key_block_size`]).
///
/// Clones share the connection pool and the cached key blocks.
#[derive(Debug, Clone)]
pub struct MongoBackend {
    client: Client,
    db: Database,
    keys: Arc<Keys>,
    key_blocks: Arc<KeyAllocator>,
}

impl MongoBackend {
    /// Connect to `url` and use database `database`.
    ///
    /// # Errors
    /// [`BackendError::Connection`] if the URL is invalid or no server can be
    /// reached.
    pub async fn connect(url: &str, database: &str) -> Result<Self, BackendError> {
        let client = Client::with_uri_str(url)
            .await
            .map_err(|e| BackendError::Connection(e.to_string()))?;
        client
            .database("admin")
            .run_command(doc! { "ping": 1_i32 })
            .await
            .map_err(|e| BackendError::Connection(e.to_string()))?;
        Ok(Self::from_client(client, database))
    }

    /// Use an existing client.
    pub fn from_client(client: Client, database: &str) -> Self {
        Self {
            db: client.database(database),
            client,
            keys: Arc::new(Keys::default()),
            key_blocks: Arc::new(KeyAllocator::default()),
        }
    }

    /// Set which column of each collection is stored as `_id` (default: a
    /// column named `id`).
    #[must_use]
    pub fn with_keys(mut self, keys: Keys) -> Self {
        self.keys = Arc::new(keys);
        self
    }

    /// Reserve generated keys `size` at a time (default 64).
    ///
    /// An insert that omits its key takes the next keys from a block this
    /// backend reserved earlier; only when the block cannot serve the whole
    /// insert does it reserve a new block of `max(rows, size)` keys with one
    /// `$inc` on the collection's `siderite_counters` document (the rest of
    /// the old block is skipped). A bulk insert always gets consecutive keys
    /// in input order. Clones of the backend and its transactions share the
    /// blocks, so call this before cloning.
    ///
    /// Keys reserved but never used are gaps: the rest of a block when the
    /// process stops, and keys of rolled-back inserts. Processes reserve
    /// separate blocks, so keys from different processes interleave and do
    /// not follow insertion order across processes. `1` restores one counter
    /// round trip per insert, with the counter always at the last key
    /// handed out; `0` is treated as `1`.
    #[must_use]
    pub fn with_key_block_size(mut self, size: u32) -> Self {
        self.key_blocks = Arc::new(KeyAllocator::new(size));
        self
    }

    /// The database this adapter works in.
    pub fn database(&self) -> &Database {
        &self.db
    }

    /// Run a database command (`db.runCommand`), the escape hatch for what
    /// plans cannot express.
    ///
    /// # Errors
    /// [`BackendError`] when the server rejects the command.
    pub async fn raw_command(&self, command: Document) -> Result<Document, OrmError> {
        self.db
            .run_command(command)
            .await
            .map_err(|e| map_error(&e).into())
    }

    /// Create a unique index on `column` of `collection`, so duplicate
    /// values fail with [`BackendError::Constraint`] like a SQL unique
    /// constraint. MongoDB enforces uniqueness only for `_id` otherwise.
    ///
    /// # Errors
    /// [`BackendError`] when the index cannot be created.
    pub async fn create_unique_index(
        &self,
        collection: &str,
        column: &str,
    ) -> Result<(), OrmError> {
        let field = if column == self.keys.pk(collection) {
            "_id"
        } else {
            column
        };
        let index = ::mongodb::IndexModel::builder()
            .keys(doc! { field: 1_i32 })
            .options(
                ::mongodb::options::IndexOptions::builder()
                    .unique(true)
                    .build(),
            )
            .build();
        self.db
            .collection::<Document>(collection)
            .create_index(index)
            .await
            .map_err(|e| OrmError::from(map_error(&e)))?;
        Ok(())
    }

    async fn run_fetch(
        &self,
        plan: &QueryPlan,
        session: Option<&mut ClientSession>,
        sink: &mut dyn RowSink,
    ) -> Result<(), OrmError> {
        let compiled = compile_query(plan, &self.keys)?;
        Io {
            db: &self.db,
            key_blocks: &self.key_blocks,
            session,
        }
        .query(compiled, sink)
        .await
    }

    async fn run_execute(
        &self,
        plan: &WritePlan,
        session: Option<&mut ClientSession>,
    ) -> Result<ExecResult, OrmError> {
        let compiled = compile_write(plan, &self.keys)?;
        Io {
            db: &self.db,
            key_blocks: &self.key_blocks,
            session,
        }
        .write(compiled)
        .await
    }
}

fn raw_unsupported() -> OrmError {
    BackendCapabilityError::from_feature(BackendKind::MongoDb, Feature::RawSql).into()
}

#[async_trait]
impl Executor for MongoBackend {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::mongodb()
    }

    async fn fetch(&self, plan: &QueryPlan) -> Result<QueryResult, OrmError> {
        let mut rows = Collect::default();
        self.run_fetch(plan, None, &mut rows).await?;
        Ok(QueryResult { rows: rows.rows })
    }

    /// Decodes each document straight from the driver's reply.
    async fn fetch_each(&self, plan: &QueryPlan, sink: &mut dyn RowSink) -> Result<(), OrmError> {
        self.run_fetch(plan, None, sink).await
    }

    async fn execute(&self, plan: &WritePlan) -> Result<ExecResult, OrmError> {
        self.run_execute(plan, None).await
    }

    async fn fetch_raw(&self, _sql: &str, _params: Vec<Value>) -> Result<QueryResult, OrmError> {
        Err(raw_unsupported())
    }

    async fn execute_raw(&self, _sql: &str, _params: Vec<Value>) -> Result<u64, OrmError> {
        Err(raw_unsupported())
    }

    async fn execute_script(&self, _sql: &str) -> Result<(), OrmError> {
        Err(raw_unsupported())
    }
}

#[async_trait]
impl Backend for MongoBackend {
    async fn begin(
        &self,
        isolation: Option<IsolationLevel>,
    ) -> Result<Box<dyn Transaction>, OrmError> {
        if let Some(level) = isolation {
            self.capabilities().require(Feature::Isolation(level))?;
        }
        let mut session = self
            .client
            .start_session()
            .await
            .map_err(|e| OrmError::from(map_error(&e)))?;
        session
            .start_transaction()
            .await
            .map_err(|e| OrmError::from(map_error(&e)))?;
        Ok(Box::new(MongoTransaction {
            backend: self.clone(),
            session: Mutex::new(Some(session)),
        }))
    }
}

/// An open MongoDB transaction. Dropped without commit, the driver aborts it.
struct MongoTransaction {
    backend: MongoBackend,
    session: Mutex<Option<ClientSession>>,
}

impl MongoTransaction {
    async fn take(&self) -> Result<ClientSession, QueryError> {
        self.session
            .lock()
            .await
            .take()
            .ok_or(QueryError::TransactionClosed)
    }
}

#[async_trait]
impl Executor for MongoTransaction {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::mongodb()
    }

    async fn fetch(&self, plan: &QueryPlan) -> Result<QueryResult, OrmError> {
        let mut rows = Collect::default();
        self.fetch_each(plan, &mut rows).await?;
        Ok(QueryResult { rows: rows.rows })
    }

    /// Decodes each document straight from the driver's reply.
    async fn fetch_each(&self, plan: &QueryPlan, sink: &mut dyn RowSink) -> Result<(), OrmError> {
        // Compile before locking: unsupported plans fail without I/O.
        let compiled = compile_query(plan, &self.backend.keys)?;
        let mut guard = self.session.lock().await;
        let session = guard.as_mut().ok_or(QueryError::TransactionClosed)?;
        Io {
            db: &self.backend.db,
            key_blocks: &self.backend.key_blocks,
            session: Some(session),
        }
        .query(compiled, sink)
        .await
    }

    async fn execute(&self, plan: &WritePlan) -> Result<ExecResult, OrmError> {
        let compiled = compile_write(plan, &self.backend.keys)?;
        let mut guard = self.session.lock().await;
        let session = guard.as_mut().ok_or(QueryError::TransactionClosed)?;
        Io {
            db: &self.backend.db,
            key_blocks: &self.backend.key_blocks,
            session: Some(session),
        }
        .write(compiled)
        .await
    }

    async fn fetch_raw(&self, _sql: &str, _params: Vec<Value>) -> Result<QueryResult, OrmError> {
        Err(raw_unsupported())
    }

    async fn execute_raw(&self, _sql: &str, _params: Vec<Value>) -> Result<u64, OrmError> {
        Err(raw_unsupported())
    }

    async fn execute_script(&self, _sql: &str) -> Result<(), OrmError> {
        Err(raw_unsupported())
    }
}

#[async_trait]
impl Transaction for MongoTransaction {
    async fn commit(&self) -> Result<(), OrmError> {
        let mut session = self.take().await?;
        session
            .commit_transaction()
            .await
            .map_err(|e| map_error(&e).into())
    }

    async fn rollback(&self) -> Result<(), OrmError> {
        let mut session = self.take().await?;
        session
            .abort_transaction()
            .await
            .map_err(|e| map_error(&e).into())
    }
}

/// Classify a driver error. Duplicate-key and validation failures are
/// constraint violations; connection-level failures are connection errors.
pub(super) fn map_error(e: &DriverError) -> BackendError {
    let message = e.to_string();
    if message.contains("E11000")
        || message.contains("code: 11000")
        || message.contains("code: 121")
    {
        return BackendError::Constraint(message);
    }
    match e.kind.as_ref() {
        ErrorKind::ServerSelection { .. }
        | ErrorKind::Io(_)
        | ErrorKind::DnsResolve { .. }
        | ErrorKind::Authentication { .. } => BackendError::Connection(message),
        _ => BackendError::Database(message),
    }
}

fn db_err(e: &DriverError) -> OrmError {
    map_error(e).into()
}

/// Database access for one statement, on a session or the implicit one.
struct Io<'a> {
    db: &'a Database,
    key_blocks: &'a KeyAllocator,
    session: Option<&'a mut ClientSession>,
}

impl Io<'_> {
    fn coll(&self, name: &str) -> Collection<Document> {
        self.db.collection(name)
    }

    fn session(&mut self) -> Option<&mut ClientSession> {
        self.session.as_deref_mut()
    }

    /// Run `read` on `collection`, handing each document to `each` as the
    /// driver reads it.
    async fn each<F>(&mut self, collection: &str, read: Read, mut each: F) -> Result<(), OrmError>
    where
        F: FnMut(&RawDocument) -> Result<(), OrmError> + Send,
    {
        let coll = self.coll(collection);
        match self.session() {
            Some(session) => {
                let mut cursor = match read {
                    Read::Find(find) => {
                        let (filter, options) = find.into_command();
                        let find = coll.find(filter).with_options(options);
                        find.session(&mut *session).await
                    }
                    Read::Pipeline(pipeline) => {
                        coll.aggregate(pipeline).session(&mut *session).await
                    }
                }
                .map_err(|e| db_err(&e))?;
                while cursor.advance(session).await.map_err(|e| db_err(&e))? {
                    each(cursor.current())?;
                }
            }
            None => {
                let mut cursor = match read {
                    Read::Find(find) => {
                        let (filter, options) = find.into_command();
                        coll.find(filter).with_options(options).await
                    }
                    Read::Pipeline(pipeline) => coll.aggregate(pipeline).await,
                }
                .map_err(|e| db_err(&e))?;
                while cursor.advance().await.map_err(|e| db_err(&e))? {
                    each(cursor.current())?;
                }
            }
        }
        Ok(())
    }

    async fn insert_many(&mut self, collection: &str, docs: &[Document]) -> Result<(), OrmError> {
        let coll = self.coll(collection);
        match self.session() {
            Some(session) => coll.insert_many(docs).session(session).await,
            None => coll.insert_many(docs).await,
        }
        .map(|_| ())
        .map_err(|e| db_err(&e))
    }

    async fn update_many(
        &mut self,
        collection: &str,
        filter: Document,
        update: UpdateSpec,
    ) -> Result<u64, OrmError> {
        let coll = self.coll(collection);
        let result = match (update, self.session()) {
            (UpdateSpec::Set(doc), Some(s)) => coll.update_many(filter, doc).session(s).await,
            (UpdateSpec::Set(doc), None) => coll.update_many(filter, doc).await,
            (UpdateSpec::Pipeline(p), Some(s)) => coll.update_many(filter, p).session(s).await,
            (UpdateSpec::Pipeline(p), None) => coll.update_many(filter, p).await,
        };
        result.map(|r| r.matched_count).map_err(|e| db_err(&e))
    }

    async fn delete_many(&mut self, collection: &str, filter: Document) -> Result<u64, OrmError> {
        let coll = self.coll(collection);
        match self.session() {
            Some(session) => coll.delete_many(filter).session(session).await,
            None => coll.delete_many(filter).await,
        }
        .map(|r| r.deleted_count)
        .map_err(|e| db_err(&e))
    }

    async fn query(
        &mut self,
        compiled: CompiledQuery,
        sink: &mut dyn RowSink,
    ) -> Result<(), OrmError> {
        let CompiledQuery {
            collection,
            read,
            columns,
            pk,
            empty_row,
        } = compiled;
        let mut decoder = Decoder::new(columns, pk);
        let mut seen = false;
        self.each(&collection, read, |doc| {
            seen = true;
            Ok(decoder.push(doc, sink)?)
        })
        .await?;
        if !seen && let Some(values) = empty_row {
            decoder.push_values(values, sink)?;
        }
        Ok(())
    }

    async fn write(&mut self, compiled: CompiledWrite) -> Result<ExecResult, OrmError> {
        match compiled {
            CompiledWrite::Insert(p) => self.insert(p).await,
            CompiledWrite::Update(p) => self.update(p).await,
            CompiledWrite::Delete(p) => self.delete(p).await,
        }
    }

    async fn insert(&mut self, plan: CompiledInsert) -> Result<ExecResult, OrmError> {
        let CompiledInsert {
            collection,
            pk,
            docs,
            returning,
        } = plan;
        if docs.is_empty() {
            return Ok(ExecResult::default());
        }
        let missing = docs.iter().filter(|d| !d.contains_key("_id")).count();
        let mut next = if missing > 0 {
            self.key_blocks
                .reserve(self.db, &collection, missing)
                .await?
        } else {
            0
        };
        let docs: Vec<Document> = docs
            .into_iter()
            .map(|doc| {
                if doc.contains_key("_id") {
                    return doc;
                }
                // `_id` first, as MongoDB stores it.
                let mut with_id = doc! { "_id": next };
                next += 1;
                with_id.extend(doc);
                with_id
            })
            .collect();
        self.insert_many(&collection, &docs).await?;
        let mut result = ExecResult {
            rows_affected: docs.len() as u64,
            returning: Vec::new(),
        };
        if !returning.is_empty() {
            // Echo what was stored (values as BSON holds them).
            let header: Arc<[String]> = returning.into();
            result.returning = docs
                .into_iter()
                .map(|d| doc_to_row(d, &header, &pk))
                .collect::<Result<_, _>>()?;
        }
        Ok(result)
    }

    async fn update(&mut self, plan: CompiledUpdate) -> Result<ExecResult, OrmError> {
        let CompiledUpdate {
            collection,
            pk,
            filter,
            update,
            returning,
        } = plan;
        if returning.is_empty() {
            let matched = self.update_many(&collection, filter, update).await?;
            return Ok(ExecResult {
                rows_affected: matched,
                returning: Vec::new(),
            });
        }
        // Pin the rows first, so the read-back is not re-matched against
        // the updated values.
        let ids = self.matching_ids(&collection, filter).await?;
        if ids.is_empty() {
            return Ok(ExecResult::default());
        }
        let by_id = doc! { "_id": { "$in": ids } };
        let matched = self.update_many(&collection, by_id.clone(), update).await?;
        let mut rows = Collect::default();
        let mut decoder = Decoder::new(returning, pk);
        self.each(&collection, find_read(by_id, None), |doc| {
            Ok(decoder.push(doc, &mut rows)?)
        })
        .await?;
        Ok(ExecResult {
            rows_affected: matched,
            returning: rows.rows,
        })
    }

    async fn delete(&mut self, plan: CompiledDelete) -> Result<ExecResult, OrmError> {
        let CompiledDelete {
            collection,
            pk,
            filter,
            returning,
        } = plan;
        if returning.is_empty() {
            let deleted = self.delete_many(&collection, filter).await?;
            return Ok(ExecResult {
                rows_affected: deleted,
                returning: Vec::new(),
            });
        }
        // Read the rows before deleting them, keeping their keys.
        let mut ids = Vec::new();
        let mut rows = Collect::default();
        let mut decoder = Decoder::new(returning, pk);
        self.each(&collection, find_read(filter, None), |doc| {
            ids.extend(raw_id(doc)?);
            Ok(decoder.push(doc, &mut rows)?)
        })
        .await?;
        if ids.is_empty() {
            return Ok(ExecResult::default());
        }
        let deleted = self
            .delete_many(&collection, doc! { "_id": { "$in": ids } })
            .await?;
        Ok(ExecResult {
            rows_affected: deleted,
            returning: rows.rows,
        })
    }

    async fn matching_ids(
        &mut self,
        collection: &str,
        filter: Document,
    ) -> Result<Vec<Bson>, OrmError> {
        let mut ids = Vec::new();
        let read = find_read(filter, Some(doc! { "_id": 1_i32 }));
        self.each(collection, read, |doc| {
            ids.extend(raw_id(doc)?);
            Ok(())
        })
        .await?;
        Ok(ids)
    }
}

/// A plain `find` of `filter`, returning `projection` (whole documents
/// when `None`).
fn find_read(filter: Document, projection: Option<Document>) -> Read {
    Read::Find(Box::new(FindQuery {
        filter,
        projection,
        ..FindQuery::default()
    }))
}
