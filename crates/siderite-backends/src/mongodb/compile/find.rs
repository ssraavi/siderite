//! Routing of simple reads to `find` instead of an aggregation pipeline.
//!
//! A `find` gets the server's fast paths (the `_id` point lookup skips
//! query planning) and costs less to set up than a pipeline, so every plan
//! a `find` can express exactly is compiled to one: a table source, no
//! grouping, aggregates or `DISTINCT`, a projection of plain root columns
//! under their own names, ordering by plain columns, and any filter. The
//! rest keeps the pipeline.

use super::filter::filter_doc;
use super::{Ctx, FindQuery, Keys, Res, check_name, invalid};
use ::mongodb::bson::{Bson, Document, doc};
use siderite_orm::{DistinctMode, Expr, OrderDirection, QueryPlan};

/// A plan compiled to a `find`.
pub(super) struct BuiltFind {
    pub(super) collection: String,
    pub(super) find: FindQuery,
    pub(super) columns: Vec<String>,
    pub(super) pk: String,
}

/// Compile `plan` to a `find`, or `None` when it needs a pipeline.
pub(super) fn find(plan: &QueryPlan, keys: &Keys) -> Res<Option<BuiltFind>> {
    if !is_simple(plan) {
        return Ok(None);
    }
    check_name(&plan.source.name)?;
    let pk = keys.pk(&plan.source.name).to_owned();
    let ctx = Ctx {
        pk: Some(&pk),
        root: plan.source.reference().as_ref(),
    };

    let mut columns = Vec::with_capacity(plan.projection.len());
    let mut projection = None;
    if !plan.projection.is_empty() {
        let mut fields = Document::new();
        for select in &plan.projection {
            let Expr::Column(column) = &select.expr else {
                return Ok(None);
            };
            if select
                .alias
                .as_ref()
                .is_some_and(|alias| *alias != column.name)
            {
                // A renamed column needs `$project`.
                return Ok(None);
            }
            fields.insert(ctx.field(column)?, 1_i32);
            columns.push(column.name.to_string());
        }
        if !fields.contains_key("_id") {
            fields.insert("_id", 0_i32);
        }
        projection = Some(fields);
    }

    let mut filter = match &plan.filter {
        Some(f) => filter_doc(f, false, &ctx)?,
        None => Document::new(),
    };
    let key_lookup = key_lookup(&filter).cloned();
    if let Some(key) = key_lookup.clone() {
        // `{_id: v}` rather than `{_id: {$eq: v}}`: the server answers the
        // bare form before parsing the query at all.
        filter = doc! { "_id": key };
    }

    let mut sort = Document::new();
    for order in &plan.ordering {
        let Expr::Column(column) = &order.expr else {
            return Ok(None);
        };
        let direction = match order.direction {
            OrderDirection::Asc => 1_i32,
            OrderDirection::Desc => -1_i32,
        };
        sort.insert(ctx.field(column)?, direction);
    }
    let sort = (!sort.is_empty() && key_lookup.is_none()).then_some(sort);

    let skip = match plan.offset {
        Some(n) if n > 0 => {
            i64::try_from(n).map_err(|_| invalid(format!("offset {n} is too large")))?;
            Some(n)
        }
        _ => None,
    };
    let limit = plan
        .limit
        .map(|n| i64::try_from(n).map_err(|_| invalid(format!("limit {n} is too large"))))
        .transpose()?;

    Ok(Some(BuiltFind {
        collection: plan.source.name.to_string(),
        find: FindQuery {
            filter,
            projection,
            sort,
            skip,
            limit,
        },
        columns,
        pk,
    }))
}

/// Whether the plan's shape fits a `find` (expressions are checked while
/// compiling).
fn is_simple(plan: &QueryPlan) -> bool {
    let distinct = match plan.distinct {
        DistinctMode::None => false,
        // Whole documents are distinct already (the pipeline ignores it too).
        DistinctMode::All => !plan.projection.is_empty(),
        DistinctMode::On(_) => true,
    };
    plan.source.subquery.is_none()
        && plan.joins.is_empty()
        && plan.lock.is_none()
        && plan.compound.is_empty()
        && !distinct
        && plan.grouping.is_empty()
        && plan.having.is_none()
        // `find` reads `limit: 0` as "no limit"; the pipeline keeps it.
        && plan.limit != Some(0)
}

/// The key when `filter` is exactly `_id` equal to one scalar: at most one
/// document matches, so any sort is redundant, and `{_id: v}` means the
/// same as `{_id: {$eq: v}}`.
fn key_lookup(filter: &Document) -> Option<&Bson> {
    if filter.len() != 1 {
        return None;
    }
    let value = match filter.get("_id")? {
        Bson::Document(op) if op.len() == 1 => op.get("$eq")?,
        other => other,
    };
    let scalar = !matches!(
        value,
        Bson::Document(_) | Bson::Array(_) | Bson::Null | Bson::RegularExpression(_)
    );
    scalar.then_some(value)
}

#[cfg(test)]
mod tests {
    use super::key_lookup;
    use ::mongodb::bson::{Bson, doc};

    #[test]
    fn key_lookup_is_one_scalar_id_equality() {
        let one = Bson::Int64(1);
        assert_eq!(key_lookup(&doc! { "_id": { "$eq": 1_i64 } }), Some(&one));
        assert_eq!(key_lookup(&doc! { "_id": 1_i64 }), Some(&one));
        let none = [
            doc! { "_id": { "$in": [1_i64] } },
            doc! { "_id": { "$eq": Bson::Null } },
            doc! { "_id": { "$eq": [1_i64] } },
            doc! { "_id": { "$eq": { "a": 1_i64 } } },
            doc! { "_id": { "$gt": 1_i64 } },
            doc! { "_id": 1_i64, "a": 1_i64 },
            doc! { "a": 1_i64 },
            doc! {},
        ];
        for filter in none {
            assert_eq!(key_lookup(&filter), None, "{filter:?}");
        }
    }
}
