use super::types::aggregate_type;
use super::*;

impl<'a> Binder<'a> {
    pub(super) fn bind_aggregate(
        &mut self,
        aggregate: &AggregateFunction,
        scope: &NameScope,
    ) -> Result<BoundAggregateFunction, RustqlError> {
        let expr = self.bind_expr(&aggregate.expr, scope)?;
        let filter = aggregate
            .filter
            .as_ref()
            .map(|filter| {
                self.bind_predicate_expr(filter, scope, "aggregate FILTER clause")
                    .map(Box::new)
            })
            .transpose()?;
        Ok(BoundAggregateFunction {
            function: aggregate.function.clone(),
            data_type: aggregate_type(&aggregate.function, &expr),
            expr: Box::new(expr),
            distinct: aggregate.distinct,
            alias: aggregate.alias.clone(),
            separator: aggregate.separator.clone(),
            percentile: aggregate.percentile,
            filter,
        })
    }

    pub(super) fn bind_group_by(
        &mut self,
        group_by: &GroupByClause,
        scope: &NameScope,
    ) -> Result<Vec<BoundExpr>, RustqlError> {
        match group_by {
            GroupByClause::Simple(exprs)
            | GroupByClause::Rollup(exprs)
            | GroupByClause::Cube(exprs) => exprs
                .iter()
                .map(|expr| self.bind_expr(expr, scope))
                .collect(),
            GroupByClause::GroupingSets(sets) => {
                let mut bound = Vec::new();
                for set in sets {
                    for expr in set {
                        bound.push(self.bind_expr(expr, scope)?);
                    }
                }
                Ok(bound)
            }
        }
    }
}

pub(super) fn group_by_from_bound(original: &GroupByClause, bound: &[BoundExpr]) -> GroupByClause {
    match original {
        GroupByClause::Simple(_) => {
            GroupByClause::Simple(bound.iter().map(|expr| expr.expr.clone()).collect())
        }
        GroupByClause::Rollup(_) => {
            GroupByClause::Rollup(bound.iter().map(|expr| expr.expr.clone()).collect())
        }
        GroupByClause::Cube(_) => {
            GroupByClause::Cube(bound.iter().map(|expr| expr.expr.clone()).collect())
        }
        GroupByClause::GroupingSets(sets) => {
            let mut idx = 0;
            let mut grouped = Vec::new();
            for set in sets {
                let mut normalized_set = Vec::new();
                for _ in set {
                    normalized_set.push(bound[idx].expr.clone());
                    idx += 1;
                }
                grouped.push(normalized_set);
            }
            GroupByClause::GroupingSets(grouped)
        }
    }
}

pub(super) fn aggregate_from_bound(bound: &BoundAggregateFunction) -> AggregateFunction {
    AggregateFunction {
        function: bound.function.clone(),
        expr: Box::new(bound.expr.expr.clone()),
        distinct: bound.distinct,
        alias: bound.alias.clone(),
        separator: bound.separator.clone(),
        percentile: bound.percentile,
        filter: bound
            .filter
            .as_ref()
            .map(|filter| Box::new(filter.expr.clone())),
    }
}
