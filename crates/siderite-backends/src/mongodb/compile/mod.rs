//! Pure compiler: plans to MongoDB `find` commands, filters, update
//! documents and pipelines.
//!
//! Nothing here touches a server; every function returns an error for a plan
//! the backend cannot express, so unsupported plans fail before any I/O.

use super::value::to_bson;
use ::mongodb::bson::{Bson, Document, doc};
use ::mongodb::options::FindOptions;
use siderite_orm::expr::{Aggregate, Column};
use siderite_orm::{
    BackendCapabilities, BackendCapabilityError, BackendKind, DeletePlan, Expr, Feature,
    InsertPlan, OrmError, QueryError, QueryPlan, UpdatePlan, Value, WritePlan,
};
use std::collections::HashMap;

mod aggregate;
mod expr;
mod filter;
mod find;
mod pipeline;

use expr::expr_bson;
use filter::filter_doc;
use pipeline::build;

/// Which column of a collection is stored as `_id`.
///
/// Plans carry no model metadata, so the adapter is told: the default is a
/// column named `id`, overridable per collection.
#[derive(Debug, Clone)]
pub struct Keys {
    default: String,
    per_collection: HashMap<String, String>,
}

impl Default for Keys {
    fn default() -> Self {
        Self {
            default: "id".into(),
            per_collection: HashMap::new(),
        }
    }
}

impl Keys {
    /// Use `column` as the primary key of `collection`.
    #[must_use]
    pub fn with(mut self, collection: impl Into<String>, column: impl Into<String>) -> Self {
        self.per_collection.insert(collection.into(), column.into());
        self
    }

    /// Use `column` for every collection without an explicit entry.
    #[must_use]
    pub fn with_default(mut self, column: impl Into<String>) -> Self {
        self.default = column.into();
        self
    }

    /// Primary-key column of `collection`.
    pub fn pk(&self, collection: &str) -> &str {
        self.per_collection
            .get(collection)
            .map_or(self.default.as_str(), String::as_str)
    }
}

/// A compiled read: run [`read`](Self::read) on `collection`.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledQuery {
    /// Collection the read runs on.
    pub collection: String,
    /// The command: a `find` for simple plans, otherwise an aggregation
    /// pipeline.
    pub read: Read,
    /// Output column names, in order. Empty means "every field of the
    /// document" (`_id` is renamed to [`pk`](Self::pk)).
    pub columns: Vec<String>,
    /// Primary-key column of the root collection.
    pub pk: String,
    /// The single row an ungrouped aggregate produces over no input (SQL
    /// returns one row, `$group` returns none). Always `None` for a `find`.
    pub empty_row: Option<Vec<Value>>,
}

/// How a compiled read runs.
///
/// A plan on a collection (not a derived table) without grouping,
/// aggregates or `DISTINCT`, whose projection names plain columns of the
/// collection under their own names and whose ordering names plain
/// columns, is a [`Find`](Read::Find); everything else is a
/// [`Pipeline`](Read::Pipeline). Both return the same rows.
#[derive(Debug, Clone, PartialEq)]
pub enum Read {
    /// A `find` command.
    Find(Box<FindQuery>),
    /// An `aggregate` command with this pipeline.
    Pipeline(Vec<Document>),
}

/// The parts of a `find` command.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FindQuery {
    /// Query filter (`{}` matches every document). A filter that is
    /// exactly one `_id` equality with a scalar is written `{_id: v}`.
    pub filter: Document,
    /// Inclusion projection (`{field: 1, ..}`, with `_id: 0` unless the key
    /// column is projected); `None` returns whole documents.
    pub projection: Option<Document>,
    /// Sort keys; `None` when the plan has no ordering, or when the filter
    /// is a single `_id` equality (at most one document matches).
    pub sort: Option<Document>,
    /// Documents to skip (`OFFSET`), absent when zero.
    pub skip: Option<u64>,
    /// Maximum number of documents (`LIMIT`), never zero.
    pub limit: Option<i64>,
}

impl FindQuery {
    /// Driver options for this `find` (everything but the filter).
    ///
    /// A limited `find` returns its whole result in the reply to `find`
    /// and the server closes the cursor, so no `getMore` or `killCursors`
    /// follows (unless the documents exceed the 16 MiB reply limit, in
    /// which case the driver fetches the rest). Up to
    /// [`FIRST_BATCH`](Self::FIRST_BATCH) documents that is the server's
    /// default; a larger limit also sets the batch size to the limit. A
    /// smaller limit sets none, because any batch size keeps the server
    /// off its express path for `_id` lookups.
    #[must_use]
    pub fn options(&self) -> FindOptions {
        let mut options = FindOptions::default();
        options.projection.clone_from(&self.projection);
        options.sort.clone_from(&self.sort);
        options.skip = self.skip;
        options.limit = self.limit;
        // The driver sends `limit + 1` when the two are equal (so the
        // server closes the cursor), which must still fit an `i32`.
        options.batch_size = self
            .limit
            .filter(|n| *n > Self::FIRST_BATCH)
            .and_then(|n| u32::try_from(n).ok())
            .filter(|n| *n < i32::MAX.unsigned_abs());
        options
    }

    /// Documents in the server's first reply to a `find` without a batch
    /// size.
    pub const FIRST_BATCH: i64 = 101;
}

/// A compiled insert.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledInsert {
    /// Target collection.
    pub collection: String,
    /// Primary-key column, stored as `_id`.
    pub pk: String,
    /// Documents in input order. `_id` is present only when the plan
    /// supplied the key column; otherwise the executor assigns keys.
    pub docs: Vec<Document>,
    /// Columns to return.
    pub returning: Vec<String>,
}

/// How an update changes documents.
#[derive(Debug, Clone, PartialEq)]
pub enum UpdateSpec {
    /// `{"$set": {..}}` with literal values.
    Set(Document),
    /// An update pipeline (`$set` stage) for F-expression assignments.
    Pipeline(Vec<Document>),
}

/// A compiled update.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledUpdate {
    /// Target collection.
    pub collection: String,
    /// Primary-key column, stored as `_id`.
    pub pk: String,
    /// Filter for `update_many`.
    pub filter: Document,
    /// Update to apply.
    pub update: UpdateSpec,
    /// Columns to return.
    pub returning: Vec<String>,
}

/// A compiled delete.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledDelete {
    /// Target collection.
    pub collection: String,
    /// Primary-key column, stored as `_id`.
    pub pk: String,
    /// Filter for `delete_many`.
    pub filter: Document,
    /// Columns to return.
    pub returning: Vec<String>,
}

/// A compiled write.
#[derive(Debug, Clone, PartialEq)]
pub enum CompiledWrite {
    /// Insert.
    Insert(CompiledInsert),
    /// Update.
    Update(CompiledUpdate),
    /// Delete.
    Delete(CompiledDelete),
}

fn invalid(message: impl Into<String>) -> OrmError {
    QueryError::InvalidPlan(message.into()).into()
}

fn unsupported(feature: Feature) -> OrmError {
    BackendCapabilityError::from_feature(BackendKind::MongoDb, feature).into()
}

type Res<T> = Result<T, OrmError>;

/// Compile a read plan.
///
/// # Errors
/// A capability error for features MongoDB lacks, or
/// [`QueryError::InvalidPlan`] for plans it cannot express.
pub fn compile_query(plan: &QueryPlan, keys: &Keys) -> Res<CompiledQuery> {
    plan.check(&BackendCapabilities::mongodb())?;
    if let Some(built) = find::find(plan, keys)? {
        return Ok(CompiledQuery {
            collection: built.collection,
            read: Read::Find(Box::new(built.find)),
            columns: built.columns,
            pk: built.pk,
            empty_row: None,
        });
    }
    let built = build(plan, keys)?;
    Ok(CompiledQuery {
        collection: built.collection,
        read: Read::Pipeline(built.pipeline),
        columns: built.columns,
        pk: built.pk,
        empty_row: built.empty_row,
    })
}

/// Compile a write plan.
///
/// # Errors
/// As [`compile_query`].
pub fn compile_write(plan: &WritePlan, keys: &Keys) -> Res<CompiledWrite> {
    plan.check(&BackendCapabilities::mongodb())?;
    Ok(match plan {
        WritePlan::Insert(p) => CompiledWrite::Insert(compile_insert(p, keys)?),
        WritePlan::Update(p) => CompiledWrite::Update(compile_update(p, keys)?),
        WritePlan::Delete(p) => CompiledWrite::Delete(compile_delete(p, keys)?),
    })
}

fn returning_columns(columns: &[siderite_orm::expr::Ident]) -> Vec<String> {
    columns.iter().map(ToString::to_string).collect()
}

fn compile_insert(plan: &InsertPlan, keys: &Keys) -> Res<CompiledInsert> {
    let pk = keys.pk(&plan.table).to_owned();
    check_name(&plan.table)?;
    let mut docs = Vec::with_capacity(plan.rows.len());
    for row in &plan.rows {
        if row.len() != plan.columns.len() {
            return Err(invalid(format!(
                "insert row has {} values for {} columns",
                row.len(),
                plan.columns.len()
            )));
        }
        let mut doc = Document::new();
        for (column, value) in plan.columns.iter().zip(row) {
            let name = if *column == pk {
                "_id".to_owned()
            } else {
                check_name(column)?;
                column.to_string()
            };
            let value = to_bson(value)?;
            if name == "_id" && value == Bson::Null {
                return Err(invalid("the primary key cannot be NULL"));
            }
            doc.insert(name, value);
        }
        docs.push(doc);
    }
    Ok(CompiledInsert {
        collection: plan.table.to_string(),
        pk,
        docs,
        returning: returning_columns(&plan.returning),
    })
}

fn compile_update(plan: &UpdatePlan, keys: &Keys) -> Res<CompiledUpdate> {
    check_name(&plan.table)?;
    let pk = keys.pk(&plan.table).to_owned();
    let ctx = Ctx {
        pk: Some(&pk),
        root: plan.table.as_ref(),
    };
    if plan.assignments.is_empty() {
        return Err(invalid("an update needs at least one assignment"));
    }
    let filter = match &plan.filter {
        Some(f) => filter_doc(f, false, &ctx)?,
        None => Document::new(),
    };
    let literal_only = plan
        .assignments
        .iter()
        .all(|(_, e)| matches!(e, Expr::Value(_)));
    let mut set = Document::new();
    for (column, expr) in &plan.assignments {
        if *column == pk {
            return Err(invalid("the primary key cannot be updated"));
        }
        check_name(column)?;
        let value = if literal_only {
            match expr {
                Expr::Value(v) => to_bson(v)?,
                _ => Bson::Null,
            }
        } else {
            expr_bson(expr, &Scope::row(&ctx))?
        };
        set.insert(column.to_string(), value);
    }
    let update = if literal_only {
        UpdateSpec::Set(doc! { "$set": set })
    } else {
        UpdateSpec::Pipeline(vec![doc! { "$set": set }])
    };
    Ok(CompiledUpdate {
        collection: plan.table.to_string(),
        pk,
        filter,
        update,
        returning: returning_columns(&plan.returning),
    })
}

fn compile_delete(plan: &DeletePlan, keys: &Keys) -> Res<CompiledDelete> {
    check_name(&plan.table)?;
    let pk = keys.pk(&plan.table).to_owned();
    let ctx = Ctx {
        pk: Some(&pk),
        root: plan.table.as_ref(),
    };
    let filter = match &plan.filter {
        Some(f) => filter_doc(f, false, &ctx)?,
        None => Document::new(),
    };
    Ok(CompiledDelete {
        collection: plan.table.to_string(),
        pk,
        filter,
        returning: returning_columns(&plan.returning),
    })
}

// ---------------------------------------------------------------------
// Names and contexts
// ---------------------------------------------------------------------

fn check_name(name: &str) -> Res<()> {
    if name.is_empty() || name.starts_with('$') || name.contains('.') || name.contains('\0') {
        return Err(invalid(format!(
            "`{name}` is not a valid MongoDB field or collection name for this backend"
        )));
    }
    Ok(())
}

/// Naming context of one pipeline level.
struct Ctx<'a> {
    /// Column stored as `_id`; `None` for the output of a derived table.
    pk: Option<&'a str>,
    /// Name the plan uses for its source (table or alias).
    root: &'a str,
}

impl Ctx<'_> {
    fn field(&self, column: &Column) -> Res<String> {
        if let Some(source) = &column.source
            && source.as_ref() != self.root
        {
            return Err(invalid(format!(
                "column `{source}.{}` refers to a source other than `{}`",
                column.name, self.root
            )));
        }
        if self.pk == Some(column.name.as_ref()) {
            return Ok("_id".into());
        }
        check_name(&column.name)?;
        Ok(column.name.to_string())
    }
}

/// Post-`$group` naming: group keys and aggregates replace source columns.
struct GroupScope {
    keys: Vec<Expr>,
    aggs: Vec<Aggregate>,
    aliases: Vec<(String, Expr)>,
}

struct Scope<'a> {
    ctx: &'a Ctx<'a>,
    group: Option<&'a GroupScope>,
}

impl<'a> Scope<'a> {
    fn row(ctx: &'a Ctx<'a>) -> Self {
        Self { ctx, group: None }
    }
}
