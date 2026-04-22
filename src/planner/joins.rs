use super::*;

impl<'a> QueryPlanner<'a> {
    pub(super) fn plan_joins(
        &self,
        left_plan: PlanNode,
        stmt: &SelectStatement,
        db: &dyn DatabaseCatalog,
        mut remaining_predicates: Vec<Expression>,
    ) -> Result<PlanNode, RustqlError> {
        let mut current_plan = left_plan;
        let base_label = stmt.from_alias.clone().unwrap_or_else(|| stmt.from.clone());
        let mut left_columns = self.infer_base_source_columns(stmt).map(|columns| {
            if stmt.from.starts_with(LATERAL_OUTER_TABLE_PREFIX) {
                columns
            } else {
                self.qualified_column_definitions(&columns, &base_label)
            }
        })?;

        for join in stmt.joins.clone() {
            if join.lateral {
                let (subquery, alias) = join.subquery.clone().ok_or_else(|| {
                    RustqlError::Internal(
                        "Planner expected LATERAL join to reference a subquery".to_string(),
                    )
                })?;
                let right_columns = self.infer_select_output_columns(&subquery)?;
                let join_condition =
                    self.join_condition_for_plan(&join, &left_columns, &right_columns, &alias);

                current_plan = self.plan_lateral_join(
                    current_plan,
                    *subquery,
                    alias.clone(),
                    self.qualified_column_names(&right_columns, &alias),
                    join.join_type.clone(),
                    join_condition,
                );
                left_columns.extend(self.qualified_column_definitions(&right_columns, &alias));
                continue;
            }

            let right_label = join
                .table_alias
                .clone()
                .unwrap_or_else(|| join.table.clone());
            let right_output_label = if join.table.starts_with(LATERAL_OUTER_TABLE_PREFIX) {
                None
            } else {
                Some(right_label.clone())
            };

            let mut pushable = Vec::new();
            let mut kept = Vec::new();
            for pred in remaining_predicates {
                let refs = self.referenced_tables(&pred);
                if refs.len() == 1 && (refs.contains(&join.table) || refs.contains(&right_label)) {
                    pushable.push(pred);
                } else {
                    kept.push(pred);
                }
            }
            remaining_predicates = kept;

            let right_filter = self.combine_conjuncts(pushable);
            let right_plan =
                self.plan_join_source(&join, right_output_label, right_filter.as_ref(), db)?;
            let right_columns = self.infer_join_source_columns(&join)?;

            let join_condition =
                self.join_condition_for_plan(&join, &left_columns, &right_columns, &right_label);
            let join_plan = if stmt.joins.len() == 1
                && matches!(join.join_type, JoinType::Inner)
                && join.using_columns.is_none()
                && !matches!(join.join_type, JoinType::Natural)
                && self.is_equality_join(&join_condition)
            {
                self.plan_hash_join(current_plan, right_plan, join_condition.clone())
            } else {
                self.plan_nested_loop_join(
                    current_plan,
                    right_plan,
                    join.join_type.clone(),
                    join_condition,
                )
            };

            current_plan = join_plan;
            if join.table.starts_with(LATERAL_OUTER_TABLE_PREFIX) {
                left_columns.extend(right_columns);
            } else {
                left_columns
                    .extend(self.qualified_column_definitions(&right_columns, &right_label));
            }
        }

        if !remaining_predicates.is_empty()
            && let Some(filter_expr) = self.combine_conjuncts(remaining_predicates)
        {
            current_plan = self.plan_filter(current_plan, filter_expr);
        }

        Ok(current_plan)
    }

    fn plan_join_source(
        &self,
        join: &Join,
        output_label: Option<String>,
        where_clause: Option<&Expression>,
        db: &dyn DatabaseCatalog,
    ) -> Result<PlanNode, RustqlError> {
        let mut plan = if let Some((subquery, alias)) = join.subquery.as_ref() {
            self.plan_subquery_access(subquery, alias, output_label)?
        } else {
            return self.plan_named_source_access(&join.table, output_label, where_clause, db);
        };

        if let Some(filter_expr) = where_clause {
            plan = self.plan_filter(plan, filter_expr.clone());
        }

        Ok(plan)
    }

    pub(super) fn join_condition_for_plan(
        &self,
        join: &Join,
        left_columns: &[ColumnDefinition],
        right_columns: &[ColumnDefinition],
        right_label: &str,
    ) -> Expression {
        if let Some(ref on_expr) = join.on {
            return on_expr.clone();
        }

        let column_pairs: Vec<(String, String)> = if let Some(ref using_columns) =
            join.using_columns
        {
            using_columns
                .iter()
                .filter_map(|column| {
                    left_columns
                        .iter()
                        .find(|left| self.unqualified_column_name(&left.name) == column.as_str())
                        .and_then(|left_col| {
                            right_columns
                                .iter()
                                .find(|right| right.name == *column)
                                .map(|right_col| {
                                    (
                                        left_col.name.clone(),
                                        format!("{}.{}", right_label, right_col.name),
                                    )
                                })
                        })
                })
                .collect()
        } else if matches!(join.join_type, JoinType::Natural) {
            left_columns
                .iter()
                .filter_map(|left_col| {
                    right_columns
                        .iter()
                        .find(|right| right.name == self.unqualified_column_name(&left_col.name))
                        .map(|right_col| {
                            (
                                left_col.name.clone(),
                                format!("{}.{}", right_label, right_col.name),
                            )
                        })
                })
                .collect()
        } else {
            Vec::new()
        };

        let mut conditions: Vec<Expression> = column_pairs
            .into_iter()
            .map(|(left, right)| Expression::BinaryOp {
                left: Box::new(Expression::Column(left)),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Column(right)),
            })
            .collect();

        if conditions.is_empty() {
            Expression::Value(Value::Boolean(true))
        } else {
            let mut condition = conditions.remove(0);
            for next in conditions {
                condition = Expression::BinaryOp {
                    left: Box::new(condition),
                    op: BinaryOperator::And,
                    right: Box::new(next),
                };
            }
            condition
        }
    }

    pub(super) fn plan_hash_join(
        &self,
        left: PlanNode,
        right: PlanNode,
        condition: Expression,
    ) -> PlanNode {
        let left_rows = self.estimate_rows(&left);
        let right_rows = self.estimate_rows(&right);
        let cost = self.estimate_hash_join_cost(&left, &right);
        let estimated_output_rows = self.estimate_join_rows(left_rows, right_rows, &condition);

        PlanNode::HashJoin {
            left: Box::new(left),
            right: Box::new(right),
            condition,
            cost,
            rows: estimated_output_rows,
        }
    }

    pub(super) fn plan_nested_loop_join(
        &self,
        left: PlanNode,
        right: PlanNode,
        join_type: JoinType,
        condition: Expression,
    ) -> PlanNode {
        let left_rows = self.estimate_rows(&left);
        let right_rows = self.estimate_rows(&right);
        let cost = self.estimate_nested_loop_join_cost(&left, &right);
        let estimated_output_rows = self.estimate_join_rows(left_rows, right_rows, &condition);

        PlanNode::NestedLoopJoin {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            condition,
            cost,
            rows: estimated_output_rows,
        }
    }

    pub(super) fn plan_lateral_join(
        &self,
        left: PlanNode,
        subquery: SelectStatement,
        alias: String,
        right_columns: Vec<String>,
        join_type: JoinType,
        condition: Expression,
    ) -> PlanNode {
        let left_rows = self.estimate_rows(&left);
        let per_left_rows = self.estimate_lateral_rows(&subquery);
        let joined_rows = left_rows.saturating_mul(per_left_rows);
        let estimated_output_rows = if matches!(join_type, JoinType::Left | JoinType::Full) {
            joined_rows.max(left_rows)
        } else {
            joined_rows
        };
        let cost = self.estimate_cost(&left)
            + left_rows as f64 * (per_left_rows as f64 * LATERAL_ROW_COST + LATERAL_FIXED_COST);

        PlanNode::LateralJoin {
            left: Box::new(left),
            subquery: Box::new(subquery),
            alias,
            right_columns,
            join_type,
            condition,
            cost,
            rows: estimated_output_rows,
        }
    }

    pub(super) fn is_equality_join(&self, condition: &Expression) -> bool {
        if let Expression::BinaryOp { op, .. } = condition {
            matches!(op, BinaryOperator::Equal)
        } else {
            false
        }
    }

    pub(super) fn estimate_lateral_rows(&self, stmt: &SelectStatement) -> usize {
        if stmt.group_by.is_some()
            || stmt
                .columns
                .iter()
                .any(|column| matches!(column, Column::Function(_)))
        {
            return 1;
        }

        stmt.fetch
            .as_ref()
            .map(|fetch| fetch.count.max(1))
            .or(stmt.limit.map(|limit| limit.max(1)))
            .unwrap_or(DEFAULT_LATERAL_ROWS)
    }
}
