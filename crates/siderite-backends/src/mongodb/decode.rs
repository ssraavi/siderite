//! Result rows from MongoDB documents.
//!
//! Reads decode each document straight from the raw BSON of the driver's
//! reply ([`Decoder`]), without building a [`Document`] first, and hand it
//! to a [`RowSink`] as [`Cells`]. Every row of a result set shares one
//! column header. Values map as [`from_bson`] maps them, `_id` is renamed
//! to the primary-key column, and a projected field the document lacks is
//! `NULL`.

use super::value::from_bson;
use ::mongodb::bson::raw::{Error as RawError, RawBsonRef, RawDocument};
use ::mongodb::bson::{Bson, Document};
use siderite_orm::{Cells, QueryError, Row, RowSink, Value};
use std::sync::Arc;

/// Decodes the documents of one result set into rows.
pub(super) struct Decoder {
    /// Projected columns; empty for "every field of the document".
    columns: Arc<[String]>,
    /// Primary-key column, stored as `_id`.
    pk: String,
    /// Header of the last whole document, reused while the fields match.
    whole: Option<Arc<[String]>>,
}

impl Decoder {
    /// Decode documents into `columns`, or into every field when `columns`
    /// is empty, with `_id` read as `pk`.
    pub(super) fn new(columns: Vec<String>, pk: String) -> Self {
        Self {
            columns: columns.into(),
            pk,
            whole: None,
        }
    }

    /// Hand `doc` to `sink` as one row.
    ///
    /// # Errors
    /// [`QueryError::Decode`] for a value with no [`Value`] counterpart or
    /// malformed BSON, or the error `sink` returns.
    pub(super) fn push(
        &mut self,
        doc: &RawDocument,
        sink: &mut dyn RowSink,
    ) -> Result<(), QueryError> {
        if self.columns.is_empty() {
            let mut row = self.whole_row(doc)?;
            return sink.push(&mut row);
        }
        sink.push(&mut Projected {
            doc,
            columns: &self.columns,
            pk: &self.pk,
        })
    }

    /// Hand the row an ungrouped aggregate produces over no input to `sink`.
    ///
    /// # Errors
    /// The error `sink` returns.
    pub(super) fn push_values(
        &self,
        values: Vec<Value>,
        sink: &mut dyn RowSink,
    ) -> Result<(), QueryError> {
        sink.push(&mut Row::with_columns(Arc::clone(&self.columns), values))
    }

    /// Every field of `doc` (`_id` renamed to the primary key, and dropped
    /// when the document also has a field of that name).
    fn whole_row(&mut self, doc: &RawDocument) -> Result<Row, QueryError> {
        let fields = doc
            .iter()
            .collect::<Result<Vec<(&str, RawBsonRef<'_>)>, _>>()
            .map_err(|e| malformed(e.key().unwrap_or_default(), &e))?;
        let pk = self.pk.as_str();
        let has_pk_field = fields.iter().any(|(key, _)| *key == pk);
        let kept = || {
            fields
                .iter()
                .filter(move |(key, _)| !(has_pk_field && *key == "_id"))
        };
        let names = || kept().map(|(key, _)| if *key == "_id" { pk } else { key });
        let header = match &self.whole {
            Some(header) if header.iter().map(String::as_str).eq(names()) => Arc::clone(header),
            _ => {
                let header: Arc<[String]> = names().map(str::to_owned).collect();
                self.whole = Some(Arc::clone(&header));
                header
            }
        };
        let values = kept()
            .zip(header.iter())
            .map(|((_, value), column)| raw_value(*value, column))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Row::with_columns(header, values))
    }
}

/// One document read against the projected columns.
struct Projected<'a> {
    doc: &'a RawDocument,
    columns: &'a Arc<[String]>,
    pk: &'a str,
}

impl Cells for Projected<'_> {
    fn columns(&self) -> &Arc<[String]> {
        self.columns
    }

    fn take(&mut self, index: usize, out: &mut Value) -> Result<bool, QueryError> {
        let Some(column) = self.columns.get(index) else {
            return Ok(false);
        };
        let key = if *column == self.pk {
            "_id"
        } else {
            column.as_str()
        };
        *out = match self.doc.get(key).map_err(|e| malformed(column, &e))? {
            Some(value) => raw_value(value, column)?,
            None => Value::Null,
        };
        Ok(true)
    }

    fn to_row(&mut self) -> Result<Row, QueryError> {
        let values = (0..self.columns.len())
            .map(|index| {
                let mut value = Value::Null;
                self.take(index, &mut value)?;
                Ok(value)
            })
            .collect::<Result<Vec<_>, QueryError>>()?;
        Ok(Row::with_columns(Arc::clone(self.columns), values))
    }
}

/// A raw BSON value as [`from_bson`] maps it.
fn raw_value(value: RawBsonRef<'_>, column: &str) -> Result<Value, QueryError> {
    let bson = Bson::try_from(value).map_err(|e| malformed(column, &e))?;
    from_bson(bson, column)
}

fn malformed(column: &str, error: &RawError) -> QueryError {
    QueryError::Decode {
        column: column.to_owned(),
        reason: format!("malformed BSON: {error}"),
    }
}

/// The row of an owned `doc` over the shared header `columns`: each
/// column's field (`_id` for `pk`), `NULL` when the document lacks it.
/// Values are moved out of the document, except for a column named again
/// later.
pub(super) fn doc_to_row(
    mut doc: Document,
    columns: &Arc<[String]>,
    pk: &str,
) -> Result<Row, QueryError> {
    let mut values = Vec::with_capacity(columns.len());
    for (i, column) in columns.iter().enumerate() {
        let key = if column == pk { "_id" } else { column.as_str() };
        let bson = if columns[i + 1..].contains(column) {
            doc.get(key).cloned()
        } else {
            doc.remove(key)
        };
        values.push(match bson {
            Some(bson) => from_bson(bson, column)?,
            None => Value::Null,
        });
    }
    Ok(Row::with_columns(Arc::clone(columns), values))
}

/// Collects rows for [`QueryResult`](siderite_orm::QueryResult)s and
/// `RETURNING` results.
#[derive(Default)]
pub(super) struct Collect {
    /// The rows, in order.
    pub(super) rows: Vec<Row>,
}

impl RowSink for Collect {
    fn push(&mut self, cells: &mut dyn Cells) -> Result<(), QueryError> {
        self.rows.push(cells.to_row()?);
        Ok(())
    }
}

/// The `_id` of `doc`, if it has one.
///
/// # Errors
/// [`QueryError::Decode`] for malformed BSON.
pub(super) fn raw_id(doc: &RawDocument) -> Result<Option<Bson>, QueryError> {
    let malformed_id = |e: RawError| malformed("_id", &e);
    doc.get("_id")
        .map_err(malformed_id)?
        .map(Bson::try_from)
        .transpose()
        .map_err(malformed_id)
}
