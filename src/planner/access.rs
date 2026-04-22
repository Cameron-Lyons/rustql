use super::*;

impl<'a> QueryPlanner<'a> {
    pub(super) fn plan_values_access(
        &self,
        values: &[Vec<Expression>],
        column_aliases: &[String],
        where_clause: Option<&Expression>,
    ) -> PlanNode {
        let width = values.first().map_or(0, Vec::len);
        let columns: Vec<String> = (0..width)
            .map(|idx| {
                column_aliases
                    .get(idx)
                    .cloned()
                    .unwrap_or_else(|| format!("column{}", idx + 1))
            })
            .collect();
        let input_rows = values.len();
        let rows = if let Some(condition) = where_clause {
            (input_rows as f64 * self.estimate_selectivity(condition, input_rows)) as usize
        } else {
            input_rows
        };
        let cost = input_rows as f64 * FUNCTION_SCAN_ROW_COST;

        PlanNode::ValuesScan {
            values: values.to_vec(),
            columns,
            filter: where_clause.cloned(),
            cost,
            rows,
        }
    }

    pub(super) fn plan_table_access(
        &self,
        table_name: &str,
        _table: &Table,
        stats: &TableStats,
        output_label: Option<String>,
        where_clause: Option<&Expression>,
        db: &dyn DatabaseCatalog,
    ) -> Result<PlanNode, RustqlError> {
        if let Some(where_expr) = where_clause
            && let Some(index_usage) = self.find_best_index(table_name, where_expr, db)
        {
            let estimated_rows = self.estimate_index_selectivity(&index_usage, db, stats);
            let cost = self.estimate_index_scan_cost(stats.row_count, estimated_rows);

            return Ok(PlanNode::IndexScan {
                table: table_name.to_string(),
                index: index_usage.index_name().to_string(),
                output_label: output_label.clone(),
                filter: Some((*where_expr).clone()),
                cost,
                rows: estimated_rows,
            });
        }

        let cost = self.estimate_seq_scan_cost(stats.row_count);
        let rows = stats.row_count;

        Ok(PlanNode::SeqScan {
            table: table_name.to_string(),
            output_label,
            filter: where_clause.cloned(),
            cost,
            rows,
        })
    }

    pub(super) fn plan_table_function_access(
        &self,
        function: &TableFunction,
        output_label: Option<String>,
        where_clause: Option<&Expression>,
    ) -> Result<PlanNode, RustqlError> {
        match function.name.as_str() {
            "generate_series" => {
                let input_rows = self
                    .estimate_generate_series_rows(function)
                    .unwrap_or(DEFAULT_GENERATE_SERIES_ROWS);
                let rows = if let Some(condition) = where_clause {
                    (input_rows as f64 * self.estimate_selectivity(condition, input_rows)) as usize
                } else {
                    input_rows
                };
                let cost = input_rows as f64 * FUNCTION_SCAN_ROW_COST;

                Ok(PlanNode::FunctionScan {
                    function: function.clone(),
                    output_label,
                    filter: where_clause.cloned(),
                    cost,
                    rows,
                })
            }
            other => Err(RustqlError::Internal(format!(
                "Unsupported table function in planner: {}",
                other
            ))),
        }
    }

    pub(super) fn plan_base_source(
        &self,
        stmt: &SelectStatement,
        output_label: Option<String>,
        where_clause: Option<&Expression>,
        db: &dyn DatabaseCatalog,
    ) -> Result<PlanNode, RustqlError> {
        if let Some((values, _alias, column_aliases)) = stmt.from_values.as_ref() {
            return Ok(self.plan_values_access(values, column_aliases, where_clause));
        }

        if let Some(function) = stmt.from_function.as_ref() {
            return self.plan_table_function_access(function, output_label, where_clause);
        }

        if let Some((subquery, alias)) = stmt.from_subquery.as_ref() {
            let mut plan = self.plan_subquery_access(subquery, alias, output_label)?;
            if let Some(filter_expr) = where_clause {
                plan = self.plan_filter(plan, filter_expr.clone());
            }
            Ok(plan)
        } else {
            self.plan_named_source_access(&stmt.from, output_label, where_clause, db)
        }
    }

    pub(super) fn plan_named_source_access(
        &self,
        source_name: &str,
        output_label: Option<String>,
        where_clause: Option<&Expression>,
        db: &dyn DatabaseCatalog,
    ) -> Result<PlanNode, RustqlError> {
        let mut plan = if let Some((cte_idx, cte)) = self.find_cte(source_name) {
            self.plan_cte_access(cte_idx, cte, output_label)?
        } else {
            if let Some(table) = db.get_table(source_name) {
                let stats = self.collect_table_stats(source_name, table, db);
                return self.plan_table_access(
                    source_name,
                    table,
                    &stats,
                    output_label,
                    where_clause,
                    db,
                );
            }

            if let Some(view) = db.get_view(source_name) {
                self.plan_view_access(source_name, &view.query_sql, output_label)?
            } else {
                return Err(RustqlError::TableNotFound(source_name.to_string()));
            }
        };

        if let Some(filter_expr) = where_clause {
            plan = self.plan_filter(plan, filter_expr.clone());
        }

        Ok(plan)
    }

    pub(super) fn plan_subquery_access(
        &self,
        subquery: &SelectStatement,
        alias: &str,
        output_label: Option<String>,
    ) -> Result<PlanNode, RustqlError> {
        let input = self.plan_select(subquery)?;
        let rows = self.estimate_rows(&input);
        let cost = self.estimate_cost(&input);

        Ok(PlanNode::SubqueryScan {
            alias: alias.to_string(),
            input: Box::new(input),
            select: Box::new(subquery.clone()),
            output_label,
            cost,
            rows,
        })
    }

    pub(super) fn plan_view_access(
        &self,
        view_name: &str,
        query_sql: &str,
        output_label: Option<String>,
    ) -> Result<PlanNode, RustqlError> {
        let view_select = self.parse_view_query(query_sql)?;
        let input = self.plan_select(&view_select)?;
        let rows = self.estimate_rows(&input);
        let cost = self.estimate_cost(&input);

        Ok(PlanNode::ViewScan {
            view: view_name.to_string(),
            input: Box::new(input),
            select: Box::new(view_select),
            output_label,
            cost,
            rows,
        })
    }

    pub(super) fn plan_cte_access(
        &self,
        cte_idx: usize,
        cte: &Cte,
        output_label: Option<String>,
    ) -> Result<PlanNode, RustqlError> {
        if cte.recursive {
            return self.plan_recursive_cte_access(cte_idx, cte, output_label);
        }

        let cte_planner = QueryPlanner {
            db: self.db,
            ctes: self.ctes[..cte_idx].to_vec(),
        };
        let input = cte_planner.plan_select(&cte.query)?;
        let rows = cte_planner.estimate_rows(&input);
        let cost = cte_planner.estimate_cost(&input);

        Ok(PlanNode::CteScan {
            cte: cte.name.clone(),
            input: Box::new(input),
            select: Box::new(cte.query.clone()),
            output_label,
            cost,
            rows,
        })
    }

    pub(super) fn plan_recursive_cte_access(
        &self,
        cte_idx: usize,
        cte: &Cte,
        output_label: Option<String>,
    ) -> Result<PlanNode, RustqlError> {
        let Some((set_op, recursive_part)) = cte.query.set_op.as_ref() else {
            let cte_planner = QueryPlanner {
                db: self.db,
                ctes: self.ctes[..cte_idx].to_vec(),
            };
            let input = cte_planner.plan_select(&cte.query)?;
            let rows = cte_planner.estimate_rows(&input);
            let cost = cte_planner.estimate_cost(&input);

            return Ok(PlanNode::CteScan {
                cte: cte.name.clone(),
                input: Box::new(input),
                select: Box::new(cte.query.clone()),
                output_label,
                cost,
                rows,
            });
        };

        if !matches!(set_op, SetOperation::UnionAll | SetOperation::Union) {
            return Err(RustqlError::Internal(
                "Recursive CTE requires UNION or UNION ALL".to_string(),
            ));
        }

        let mut base_select = cte.query.clone();
        base_select.set_op = None;
        let cte_planner = QueryPlanner {
            db: self.db,
            ctes: self.ctes[..cte_idx].to_vec(),
        };
        let base = cte_planner.plan_select(&base_select)?;
        let base_rows = cte_planner.estimate_rows(&base);
        let rows = base_rows
            .saturating_mul(DEFAULT_LATERAL_ROWS)
            .max(base_rows);
        let cost = cte_planner.estimate_cost(&base) + rows as f64;

        Ok(PlanNode::RecursiveCteScan {
            cte: cte.name.clone(),
            base: Box::new(base),
            base_select: Box::new(base_select),
            recursive_select: recursive_part.clone(),
            union_all: matches!(set_op, SetOperation::UnionAll),
            output_label,
            cost,
            rows,
        })
    }

    pub(super) fn find_cte(&self, source_name: &str) -> Option<(usize, &Cte)> {
        self.ctes
            .iter()
            .enumerate()
            .rev()
            .find(|(_, cte)| cte.name == source_name)
    }
}
