use crate::ast::*;
use crate::database::DatabaseCatalog;
use crate::error::RustqlError;
use crate::executor::aggregate::format_aggregate_header;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundType {
    Known(DataType),
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundColumnRef {
    pub relation: Option<String>,
    pub name: String,
    pub qualified_name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub outer: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundExpr {
    pub kind: BoundExprKind,
    pub data_type: BoundType,
    pub nullable: bool,
    pub expr: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundExprKind {
    Column(BoundColumnRef),
    Alias {
        name: String,
    },
    Value(Value),
    BinaryOp {
        left: Box<BoundExpr>,
        op: BinaryOperator,
        right: Box<BoundExpr>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<BoundExpr>,
    },
    InList {
        left: Box<BoundExpr>,
        values: Vec<Value>,
    },
    IsNull {
        expr: Box<BoundExpr>,
        not: bool,
    },
    Subquery(Box<BoundSelectStatement>),
    Exists(Box<BoundSelectStatement>),
    Any {
        left: Box<BoundExpr>,
        op: BinaryOperator,
        subquery: Box<BoundSelectStatement>,
    },
    All {
        left: Box<BoundExpr>,
        op: BinaryOperator,
        subquery: Box<BoundSelectStatement>,
    },
    Aggregate(Box<BoundAggregateFunction>),
    ScalarFunction {
        name: ScalarFunctionType,
        args: Vec<BoundExpr>,
    },
    WindowFunction {
        function: WindowFunctionType,
        args: Vec<BoundExpr>,
        partition_by: Vec<BoundExpr>,
        order_by: Vec<BoundOrderByExpr>,
        frame: Option<WindowFrame>,
    },
    Cast {
        expr: Box<BoundExpr>,
        data_type: DataType,
    },
    Case {
        operand: Option<Box<BoundExpr>>,
        when_clauses: Vec<(BoundExpr, BoundExpr)>,
        else_clause: Option<Box<BoundExpr>>,
    },
    IsDistinctFrom {
        left: Box<BoundExpr>,
        right: Box<BoundExpr>,
        not: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundAggregateFunction {
    pub function: AggregateFunctionType,
    pub expr: Box<BoundExpr>,
    pub distinct: bool,
    pub alias: Option<String>,
    pub separator: Option<String>,
    pub percentile: Option<f64>,
    pub filter: Option<Box<BoundExpr>>,
    pub data_type: BoundType,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundOrderByExpr {
    pub expr: BoundExpr,
    pub asc: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundSelectItem {
    Wildcard {
        columns: Vec<BoundColumnRef>,
    },
    Column {
        reference: BoundColumnRef,
        alias: Option<String>,
        output: ColumnDefinition,
    },
    Expression {
        expr: BoundExpr,
        alias: Option<String>,
        output: ColumnDefinition,
    },
    Aggregate {
        aggregate: BoundAggregateFunction,
        output: ColumnDefinition,
    },
    Subquery {
        select: Box<BoundSelectStatement>,
        output: ColumnDefinition,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundSelectStatement {
    pub statement: SelectStatement,
    pub source_columns: Vec<BoundColumnRef>,
    pub output_columns: Vec<ColumnDefinition>,
    pub select_items: Vec<BoundSelectItem>,
    pub where_clause: Option<BoundExpr>,
    pub group_by: Option<Vec<BoundExpr>>,
    pub having: Option<BoundExpr>,
    pub order_by: Option<Vec<BoundOrderByExpr>>,
    pub distinct_on: Option<Vec<BoundExpr>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundStatement {
    Select(BoundSelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    AlterTable(AlterTableStatement),
    CreateIndex(CreateIndexStatement),
    DropIndex(DropIndexStatement),
    TruncateTable { table_name: String },
    CreateView { name: String, query_sql: String },
    DropView { name: String, if_exists: bool },
    BeginTransaction,
    CommitTransaction,
    RollbackTransaction,
    Savepoint(String),
    ReleaseSavepoint(String),
    RollbackToSavepoint(String),
    Explain(BoundSelectStatement),
    ExplainAnalyze(BoundSelectStatement),
    Describe(String),
    ShowTables,
    Analyze(String),
    Merge(MergeStatement),
    Do { statements: Vec<Statement> },
}

impl BoundStatement {
    pub fn into_statement(self) -> Statement {
        match self {
            BoundStatement::Select(stmt) => Statement::Select(stmt.statement),
            BoundStatement::Insert(stmt) => Statement::Insert(stmt),
            BoundStatement::Update(stmt) => Statement::Update(stmt),
            BoundStatement::Delete(stmt) => Statement::Delete(stmt),
            BoundStatement::CreateTable(stmt) => Statement::CreateTable(stmt),
            BoundStatement::DropTable(stmt) => Statement::DropTable(stmt),
            BoundStatement::AlterTable(stmt) => Statement::AlterTable(stmt),
            BoundStatement::CreateIndex(stmt) => Statement::CreateIndex(stmt),
            BoundStatement::DropIndex(stmt) => Statement::DropIndex(stmt),
            BoundStatement::TruncateTable { table_name } => Statement::TruncateTable { table_name },
            BoundStatement::CreateView { name, query_sql } => {
                Statement::CreateView { name, query_sql }
            }
            BoundStatement::DropView { name, if_exists } => Statement::DropView { name, if_exists },
            BoundStatement::BeginTransaction => Statement::BeginTransaction,
            BoundStatement::CommitTransaction => Statement::CommitTransaction,
            BoundStatement::RollbackTransaction => Statement::RollbackTransaction,
            BoundStatement::Savepoint(name) => Statement::Savepoint(name),
            BoundStatement::ReleaseSavepoint(name) => Statement::ReleaseSavepoint(name),
            BoundStatement::RollbackToSavepoint(name) => Statement::RollbackToSavepoint(name),
            BoundStatement::Explain(stmt) => Statement::Explain(stmt.statement),
            BoundStatement::ExplainAnalyze(stmt) => Statement::ExplainAnalyze(stmt.statement),
            BoundStatement::Describe(name) => Statement::Describe(name),
            BoundStatement::ShowTables => Statement::ShowTables,
            BoundStatement::Analyze(name) => Statement::Analyze(name),
            BoundStatement::Merge(stmt) => Statement::Merge(stmt),
            BoundStatement::Do { statements } => Statement::Do { statements },
        }
    }
}

#[derive(Debug, Clone, Default)]
struct NameScope {
    columns: Vec<BoundColumnRef>,
}

struct BoundSelectItems {
    select_items: Vec<BoundSelectItem>,
    output_columns: Vec<ColumnDefinition>,
    aliases: Vec<(String, BoundExpr)>,
    normalized_columns: Vec<Column>,
}

pub struct Binder<'a> {
    db: &'a dyn DatabaseCatalog,
    ctes: Vec<Cte>,
    outer_columns: Vec<BoundColumnRef>,
    binding_ctes: Vec<String>,
}

pub fn bind_statement(
    db: &dyn DatabaseCatalog,
    statement: Statement,
) -> Result<BoundStatement, RustqlError> {
    Binder::new(db).bind_statement(statement)
}

pub fn bind_select(
    db: &dyn DatabaseCatalog,
    stmt: &SelectStatement,
) -> Result<BoundSelectStatement, RustqlError> {
    Binder::new(db).bind_select(stmt)
}

pub(crate) fn bind_select_with_ctes(
    db: &dyn DatabaseCatalog,
    ctes: Vec<Cte>,
    stmt: &SelectStatement,
) -> Result<BoundSelectStatement, RustqlError> {
    Binder::with_ctes(db, ctes).bind_select(stmt)
}

impl<'a> Binder<'a> {
    pub fn new(db: &'a dyn DatabaseCatalog) -> Self {
        Self {
            db,
            ctes: Vec::new(),
            outer_columns: Vec::new(),
            binding_ctes: Vec::new(),
        }
    }

    pub(crate) fn with_ctes(db: &'a dyn DatabaseCatalog, ctes: Vec<Cte>) -> Self {
        Self {
            db,
            ctes,
            outer_columns: Vec::new(),
            binding_ctes: Vec::new(),
        }
    }

    pub fn bind_statement(&mut self, statement: Statement) -> Result<BoundStatement, RustqlError> {
        match statement {
            Statement::Select(stmt) => Ok(BoundStatement::Select(self.bind_select(&stmt)?)),
            Statement::Insert(stmt) => Ok(BoundStatement::Insert(self.bind_insert(stmt)?)),
            Statement::Update(stmt) => Ok(BoundStatement::Update(self.bind_update(stmt)?)),
            Statement::Delete(stmt) => Ok(BoundStatement::Delete(self.bind_delete(stmt)?)),
            Statement::CreateTable(stmt) => {
                Ok(BoundStatement::CreateTable(self.bind_create_table(stmt)?))
            }
            Statement::DropTable(stmt) => Ok(BoundStatement::DropTable(stmt)),
            Statement::AlterTable(stmt) => Ok(BoundStatement::AlterTable(stmt)),
            Statement::CreateIndex(stmt) => {
                Ok(BoundStatement::CreateIndex(self.bind_create_index(stmt)?))
            }
            Statement::DropIndex(stmt) => Ok(BoundStatement::DropIndex(stmt)),
            Statement::TruncateTable { table_name } => {
                Ok(BoundStatement::TruncateTable { table_name })
            }
            Statement::CreateView { name, query_sql } => {
                Ok(BoundStatement::CreateView { name, query_sql })
            }
            Statement::DropView { name, if_exists } => {
                Ok(BoundStatement::DropView { name, if_exists })
            }
            Statement::BeginTransaction => Ok(BoundStatement::BeginTransaction),
            Statement::CommitTransaction => Ok(BoundStatement::CommitTransaction),
            Statement::RollbackTransaction => Ok(BoundStatement::RollbackTransaction),
            Statement::Savepoint(name) => Ok(BoundStatement::Savepoint(name)),
            Statement::ReleaseSavepoint(name) => Ok(BoundStatement::ReleaseSavepoint(name)),
            Statement::RollbackToSavepoint(name) => Ok(BoundStatement::RollbackToSavepoint(name)),
            Statement::Explain(stmt) => Ok(BoundStatement::Explain(self.bind_select(&stmt)?)),
            Statement::ExplainAnalyze(stmt) => {
                Ok(BoundStatement::ExplainAnalyze(self.bind_select(&stmt)?))
            }
            Statement::Describe(name) => Ok(BoundStatement::Describe(name)),
            Statement::ShowTables => Ok(BoundStatement::ShowTables),
            Statement::Analyze(name) => Ok(BoundStatement::Analyze(name)),
            Statement::Merge(stmt) => Ok(BoundStatement::Merge(self.bind_merge(stmt)?)),
            Statement::Do { statements } => Ok(BoundStatement::Do { statements }),
        }
    }

    pub fn bind_select(
        &mut self,
        stmt: &SelectStatement,
    ) -> Result<BoundSelectStatement, RustqlError> {
        let mut stmt = stmt.clone();
        crate::executor::select::resolve_window_definitions(&mut stmt);

        let cte_len = self.ctes.len();
        self.ctes.extend(stmt.ctes.clone());

        let result = self.bind_select_in_current_scope(&stmt);
        self.ctes.truncate(cte_len);
        result
    }

    fn bind_select_in_current_scope(
        &mut self,
        stmt: &SelectStatement,
    ) -> Result<BoundSelectStatement, RustqlError> {
        if let Some((op, right_stmt)) = stmt.set_op.as_ref() {
            let mut left_stmt = stmt.clone();
            left_stmt.set_op = None;
            left_stmt.ctes.clear();
            let left = self.bind_select_in_current_scope(&left_stmt)?;
            let right = self.bind_select(right_stmt)?;
            if left.output_columns.len() != right.output_columns.len() {
                return Err(RustqlError::TypeMismatch(format!(
                    "Set operation inputs must have the same number of columns: left has {}, right has {}",
                    left.output_columns.len(),
                    right.output_columns.len()
                )));
            }

            let mut normalized = stmt.clone();
            normalized.set_op = Some((op.clone(), Box::new(right.statement.clone())));
            return Ok(BoundSelectStatement {
                statement: normalized,
                source_columns: left.source_columns.clone(),
                output_columns: left.output_columns.clone(),
                select_items: left.select_items.clone(),
                where_clause: left.where_clause.clone(),
                group_by: left.group_by.clone(),
                having: left.having.clone(),
                order_by: left.order_by.clone(),
                distinct_on: left.distinct_on.clone(),
            });
        }

        let mut normalized = stmt.clone();
        let source_columns = self.bind_select_sources(&mut normalized)?;
        let scope = NameScope {
            columns: source_columns.clone(),
        };

        let BoundSelectItems {
            select_items,
            output_columns,
            aliases,
            normalized_columns,
        } = self.bind_select_items(&normalized, &scope)?;
        normalized.columns = normalized_columns;

        let where_clause = if let Some(expr) = normalized.where_clause.as_ref() {
            let bound = self.bind_predicate_expr(expr, &scope, "WHERE clause")?;
            normalized.where_clause = Some(bound.expr.clone());
            Some(bound)
        } else {
            None
        };

        let group_by = if let Some(group_by) = normalized.group_by.as_ref() {
            let bound = self.bind_group_by(group_by, &scope)?;
            normalized.group_by = Some(group_by_from_bound(group_by, &bound));
            Some(bound)
        } else {
            None
        };

        let having = if let Some(expr) = normalized.having.as_ref() {
            let bound = self.bind_predicate_expr(expr, &scope, "HAVING clause")?;
            normalized.having = Some(bound.expr.clone());
            Some(bound)
        } else {
            None
        };

        let distinct_on = if let Some(exprs) = normalized.distinct_on.as_ref() {
            let bound = exprs
                .iter()
                .map(|expr| self.bind_expr(expr, &scope))
                .collect::<Result<Vec<_>, _>>()?;
            normalized.distinct_on = Some(bound.iter().map(|expr| expr.expr.clone()).collect());
            Some(bound)
        } else {
            None
        };

        let order_by = if let Some(order_by) = normalized.order_by.as_ref() {
            let bound = order_by
                .iter()
                .map(|item| {
                    let expr = self.bind_order_by_expr(&item.expr, &scope, &aliases)?;
                    Ok(BoundOrderByExpr {
                        expr,
                        asc: item.asc,
                    })
                })
                .collect::<Result<Vec<_>, RustqlError>>()?;
            normalized.order_by = Some(
                bound
                    .iter()
                    .map(|item| OrderByExpr {
                        expr: item.expr.expr.clone(),
                        asc: item.asc,
                    })
                    .collect(),
            );
            Some(bound)
        } else {
            None
        };

        Ok(BoundSelectStatement {
            statement: normalized,
            source_columns,
            output_columns,
            select_items,
            where_clause,
            group_by,
            having,
            order_by,
            distinct_on,
        })
    }

    fn bind_select_sources(
        &mut self,
        stmt: &mut SelectStatement,
    ) -> Result<Vec<BoundColumnRef>, RustqlError> {
        let mut columns = Vec::new();

        if let Some((subquery, alias)) = stmt.from_subquery.as_mut() {
            let bound = self.bind_child_select(subquery, Vec::new())?;
            **subquery = bound.statement.clone();
            columns = self.columns_for_relation(&bound.output_columns, alias, true);
        } else if let Some((rows, alias, column_aliases)) = stmt.from_values.as_ref() {
            columns = self.bind_values_source(rows, alias, column_aliases)?;
        } else if let Some(function) = stmt.from_function.as_mut() {
            let source = self.bind_table_function(function)?;
            columns = source;
        } else if !stmt.from.is_empty() {
            let label = stmt.from_alias.as_deref().unwrap_or(&stmt.from);
            columns = self.source_columns_for_name(&stmt.from, label)?;
        }

        for join in &mut stmt.joins {
            let right_columns = if let Some((subquery, alias)) = join.subquery.as_mut() {
                let outer = if join.lateral {
                    self.outer_for_child(&columns)
                } else {
                    Vec::new()
                };
                let bound = self.bind_child_select(subquery, outer)?;
                **subquery = bound.statement.clone();
                self.columns_for_relation(&bound.output_columns, alias, true)
            } else {
                let label = join.table_alias.as_deref().unwrap_or(&join.table);
                self.source_columns_for_name(&join.table, label)?
            };

            self.validate_join_using(join, &columns, &right_columns)?;

            let mut combined = columns.clone();
            combined.extend(right_columns.clone());
            let join_scope = NameScope { columns: combined };
            if let Some(on_expr) = join.on.as_ref() {
                let bound = self.bind_predicate_expr(on_expr, &join_scope, "JOIN ON clause")?;
                join.on = Some(bound.expr);
            }

            columns.extend(right_columns);
        }

        Ok(columns)
    }

    fn bind_select_items(
        &mut self,
        stmt: &SelectStatement,
        scope: &NameScope,
    ) -> Result<BoundSelectItems, RustqlError> {
        let mut items = Vec::new();
        let mut outputs = Vec::new();
        let mut aliases = Vec::new();
        let mut normalized_columns = Vec::new();

        for column in &stmt.columns {
            match column {
                Column::All => {
                    let output_columns = self.star_output_columns(stmt, scope);
                    outputs.extend(output_columns);
                    items.push(BoundSelectItem::Wildcard {
                        columns: scope.columns.clone(),
                    });
                    normalized_columns.push(Column::All);
                }
                Column::Named { name, alias } => {
                    let reference = self.resolve_column(name, scope)?;
                    let output = column_definition(
                        alias.clone().unwrap_or_else(|| name.clone()),
                        reference.data_type.clone(),
                        reference.nullable,
                    );
                    if let Some(alias) = alias {
                        aliases.push((
                            alias.clone(),
                            BoundExpr {
                                kind: BoundExprKind::Column(reference.clone()),
                                data_type: column_ref_type(&reference),
                                nullable: reference.nullable,
                                expr: Expression::Column(name.clone()),
                            },
                        ));
                    }
                    outputs.push(output.clone());
                    items.push(BoundSelectItem::Column {
                        reference,
                        alias: alias.clone(),
                        output,
                    });
                    normalized_columns.push(column.clone());
                }
                Column::Expression { expr, alias } => {
                    let bound = self.bind_expr(expr, scope)?;
                    let output = column_definition(
                        alias.clone().unwrap_or_else(|| "<expression>".to_string()),
                        bound_type_or_text(&bound.data_type),
                        bound.nullable,
                    );
                    if let Some(alias) = alias {
                        aliases.push((alias.clone(), bound.clone()));
                    }
                    outputs.push(output.clone());
                    items.push(BoundSelectItem::Expression {
                        expr: bound.clone(),
                        alias: alias.clone(),
                        output,
                    });
                    normalized_columns.push(Column::Expression {
                        expr: bound.expr,
                        alias: alias.clone(),
                    });
                }
                Column::Function(aggregate) => {
                    let bound = self.bind_aggregate(aggregate, scope)?;
                    let output = column_definition(
                        format_aggregate_header(aggregate),
                        bound_type_or_text(&bound.data_type),
                        true,
                    );
                    if let Some(alias) = aggregate.alias.as_ref() {
                        aliases.push((
                            alias.clone(),
                            BoundExpr {
                                kind: BoundExprKind::Aggregate(Box::new(bound.clone())),
                                data_type: bound.data_type.clone(),
                                nullable: true,
                                expr: Expression::Function(aggregate.clone()),
                            },
                        ));
                    }
                    outputs.push(output.clone());
                    items.push(BoundSelectItem::Aggregate {
                        aggregate: bound.clone(),
                        output,
                    });
                    normalized_columns.push(Column::Function(aggregate_from_bound(&bound)));
                }
                Column::Subquery(subquery) => {
                    let bound = self.bind_correlated_select(subquery, scope)?;
                    ensure_single_column_subquery(&bound, "Scalar subquery")?;
                    let output_type = bound
                        .output_columns
                        .first()
                        .map(|column| column.data_type.clone())
                        .unwrap_or(DataType::Text);
                    let output = column_definition("<subquery>".to_string(), output_type, true);
                    outputs.push(output.clone());
                    items.push(BoundSelectItem::Subquery {
                        select: Box::new(bound.clone()),
                        output,
                    });
                    normalized_columns.push(Column::Subquery(Box::new(bound.statement)));
                }
            }
        }

        Ok(BoundSelectItems {
            select_items: items,
            output_columns: outputs,
            aliases,
            normalized_columns,
        })
    }

    fn bind_expr(
        &mut self,
        expr: &Expression,
        scope: &NameScope,
    ) -> Result<BoundExpr, RustqlError> {
        match expr {
            Expression::Column(name) => {
                if name == "*" {
                    return Ok(BoundExpr {
                        kind: BoundExprKind::Value(Value::Integer(1)),
                        data_type: BoundType::Known(DataType::Integer),
                        nullable: false,
                        expr: expr.clone(),
                    });
                }
                let reference = self.resolve_column(name, scope)?;
                Ok(BoundExpr {
                    kind: BoundExprKind::Column(reference.clone()),
                    data_type: column_ref_type(&reference),
                    nullable: reference.nullable,
                    expr: expr.clone(),
                })
            }
            Expression::Value(value) => Ok(BoundExpr {
                kind: BoundExprKind::Value(value.clone()),
                data_type: value_type(value),
                nullable: matches!(value, Value::Null),
                expr: expr.clone(),
            }),
            Expression::BinaryOp { left, op, right } => {
                if matches!(op, BinaryOperator::Between) {
                    let left = self.bind_expr(left, scope)?;
                    let Expression::BinaryOp {
                        left: lower,
                        op: lower_op,
                        right: upper,
                    } = right.as_ref()
                    else {
                        return Err(RustqlError::TypeMismatch(
                            "BETWEEN requires lower and upper bounds".to_string(),
                        ));
                    };
                    if !matches!(lower_op, BinaryOperator::And) {
                        return Err(RustqlError::TypeMismatch(
                            "BETWEEN requires lower and upper bounds".to_string(),
                        ));
                    }
                    let lower = self.bind_expr(lower, scope)?;
                    let upper = self.bind_expr(upper, scope)?;
                    ensure_comparable(&left.data_type, &lower.data_type, "BETWEEN")?;
                    ensure_comparable(&left.data_type, &upper.data_type, "BETWEEN")?;
                    let right = BoundExpr {
                        kind: BoundExprKind::BinaryOp {
                            left: Box::new(lower.clone()),
                            op: BinaryOperator::And,
                            right: Box::new(upper.clone()),
                        },
                        data_type: BoundType::Unknown,
                        nullable: lower.nullable || upper.nullable,
                        expr: Expression::BinaryOp {
                            left: Box::new(lower.expr.clone()),
                            op: BinaryOperator::And,
                            right: Box::new(upper.expr.clone()),
                        },
                    };
                    return Ok(BoundExpr {
                        kind: BoundExprKind::BinaryOp {
                            left: Box::new(left.clone()),
                            op: op.clone(),
                            right: Box::new(right.clone()),
                        },
                        data_type: BoundType::Known(DataType::Boolean),
                        nullable: false,
                        expr: Expression::BinaryOp {
                            left: Box::new(left.expr),
                            op: op.clone(),
                            right: Box::new(right.expr),
                        },
                    });
                }

                let left = self.bind_expr(left, scope)?;
                let right = self.bind_expr(right, scope)?;
                let data_type = self.bind_binary_type(op, &left, &right)?;
                let normalized = Expression::BinaryOp {
                    left: Box::new(left.expr.clone()),
                    op: op.clone(),
                    right: Box::new(right.expr.clone()),
                };
                Ok(BoundExpr {
                    kind: BoundExprKind::BinaryOp {
                        left: Box::new(left.clone()),
                        op: op.clone(),
                        right: Box::new(right.clone()),
                    },
                    data_type,
                    nullable: left.nullable || right.nullable,
                    expr: normalized,
                })
            }
            Expression::UnaryOp { op, expr } => {
                let bound = self.bind_expr(expr, scope)?;
                let data_type = match op {
                    UnaryOperator::Not => {
                        ensure_boolean(&bound, "NOT operand")?;
                        BoundType::Known(DataType::Boolean)
                    }
                    UnaryOperator::Minus => {
                        ensure_numeric(&bound, "Unary minus")?;
                        bound.data_type.clone()
                    }
                };
                Ok(BoundExpr {
                    kind: BoundExprKind::UnaryOp {
                        op: op.clone(),
                        expr: Box::new(bound.clone()),
                    },
                    data_type,
                    nullable: bound.nullable,
                    expr: Expression::UnaryOp {
                        op: op.clone(),
                        expr: Box::new(bound.expr),
                    },
                })
            }
            Expression::In { left, values } => {
                let left = self.bind_expr(left, scope)?;
                for value in values {
                    ensure_comparable(&left.data_type, &value_type(value), "IN")?;
                }
                Ok(BoundExpr {
                    kind: BoundExprKind::InList {
                        left: Box::new(left.clone()),
                        values: values.clone(),
                    },
                    data_type: BoundType::Known(DataType::Boolean),
                    nullable: false,
                    expr: Expression::In {
                        left: Box::new(left.expr),
                        values: values.clone(),
                    },
                })
            }
            Expression::IsNull { expr, not } => {
                let bound = self.bind_expr(expr, scope)?;
                Ok(BoundExpr {
                    kind: BoundExprKind::IsNull {
                        expr: Box::new(bound.clone()),
                        not: *not,
                    },
                    data_type: BoundType::Known(DataType::Boolean),
                    nullable: false,
                    expr: Expression::IsNull {
                        expr: Box::new(bound.expr),
                        not: *not,
                    },
                })
            }
            Expression::Subquery(subquery) => {
                let bound = self.bind_correlated_select(subquery, scope)?;
                ensure_single_column_subquery(&bound, "Scalar subquery")?;
                let output = bound.output_columns.first();
                Ok(BoundExpr {
                    kind: BoundExprKind::Subquery(Box::new(bound.clone())),
                    data_type: output
                        .map(|column| BoundType::Known(column.data_type.clone()))
                        .unwrap_or(BoundType::Unknown),
                    nullable: true,
                    expr: Expression::Subquery(Box::new(bound.statement)),
                })
            }
            Expression::Exists(subquery) => {
                let bound = self.bind_correlated_select(subquery, scope)?;
                Ok(BoundExpr {
                    kind: BoundExprKind::Exists(Box::new(bound.clone())),
                    data_type: BoundType::Known(DataType::Boolean),
                    nullable: false,
                    expr: Expression::Exists(Box::new(bound.statement)),
                })
            }
            Expression::Any { left, op, subquery } => {
                let left = self.bind_expr(left, scope)?;
                let subquery = self.bind_correlated_select(subquery, scope)?;
                ensure_single_column_subquery(&subquery, "ANY subquery")?;
                if let Some(output) = subquery.output_columns.first() {
                    ensure_comparable(
                        &left.data_type,
                        &BoundType::Known(output.data_type.clone()),
                        "ANY",
                    )?;
                }
                Ok(BoundExpr {
                    kind: BoundExprKind::Any {
                        left: Box::new(left.clone()),
                        op: op.clone(),
                        subquery: Box::new(subquery.clone()),
                    },
                    data_type: BoundType::Known(DataType::Boolean),
                    nullable: false,
                    expr: Expression::Any {
                        left: Box::new(left.expr),
                        op: op.clone(),
                        subquery: Box::new(subquery.statement),
                    },
                })
            }
            Expression::All { left, op, subquery } => {
                let left = self.bind_expr(left, scope)?;
                let subquery = self.bind_correlated_select(subquery, scope)?;
                ensure_single_column_subquery(&subquery, "ALL subquery")?;
                if let Some(output) = subquery.output_columns.first() {
                    ensure_comparable(
                        &left.data_type,
                        &BoundType::Known(output.data_type.clone()),
                        "ALL",
                    )?;
                }
                Ok(BoundExpr {
                    kind: BoundExprKind::All {
                        left: Box::new(left.clone()),
                        op: op.clone(),
                        subquery: Box::new(subquery.clone()),
                    },
                    data_type: BoundType::Known(DataType::Boolean),
                    nullable: false,
                    expr: Expression::All {
                        left: Box::new(left.expr),
                        op: op.clone(),
                        subquery: Box::new(subquery.statement),
                    },
                })
            }
            Expression::Function(aggregate) => {
                let bound = self.bind_aggregate(aggregate, scope)?;
                Ok(BoundExpr {
                    kind: BoundExprKind::Aggregate(Box::new(bound.clone())),
                    data_type: bound.data_type.clone(),
                    nullable: true,
                    expr: Expression::Function(aggregate_from_bound(&bound)),
                })
            }
            Expression::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                let operand = operand
                    .as_ref()
                    .map(|expr| self.bind_expr(expr, scope).map(Box::new))
                    .transpose()?;
                let mut bound_clauses = Vec::with_capacity(when_clauses.len());
                let mut result_type = BoundType::Unknown;
                for (when_expr, then_expr) in when_clauses {
                    let when = self.bind_expr(when_expr, scope)?;
                    if operand.is_none() {
                        ensure_boolean(&when, "CASE WHEN condition")?;
                    } else if let Some(operand) = operand.as_ref() {
                        ensure_comparable(&operand.data_type, &when.data_type, "CASE operand")?;
                    }
                    let then = self.bind_expr(then_expr, scope)?;
                    result_type = common_type(&result_type, &then.data_type);
                    bound_clauses.push((when, then));
                }
                let else_clause = else_clause
                    .as_ref()
                    .map(|expr| self.bind_expr(expr, scope).map(Box::new))
                    .transpose()?;
                if let Some(else_expr) = else_clause.as_ref() {
                    result_type = common_type(&result_type, &else_expr.data_type);
                }
                Ok(BoundExpr {
                    kind: BoundExprKind::Case {
                        operand: operand.clone(),
                        when_clauses: bound_clauses.clone(),
                        else_clause: else_clause.clone(),
                    },
                    data_type: result_type,
                    nullable: true,
                    expr: Expression::Case {
                        operand: operand.map(|expr| Box::new(expr.expr)),
                        when_clauses: bound_clauses
                            .into_iter()
                            .map(|(when, then)| (when.expr, then.expr))
                            .collect(),
                        else_clause: else_clause.map(|expr| Box::new(expr.expr)),
                    },
                })
            }
            Expression::ScalarFunction { name, args } => {
                let args = args
                    .iter()
                    .map(|arg| self.bind_expr(arg, scope))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(BoundExpr {
                    kind: BoundExprKind::ScalarFunction {
                        name: name.clone(),
                        args: args.clone(),
                    },
                    data_type: scalar_function_type(name, &args),
                    nullable: true,
                    expr: Expression::ScalarFunction {
                        name: name.clone(),
                        args: args.into_iter().map(|arg| arg.expr).collect(),
                    },
                })
            }
            Expression::WindowFunction {
                function,
                args,
                partition_by,
                order_by,
                frame,
            } => {
                let args = args
                    .iter()
                    .map(|arg| self.bind_expr(arg, scope))
                    .collect::<Result<Vec<_>, _>>()?;
                let partition_by = partition_by
                    .iter()
                    .map(|expr| self.bind_expr(expr, scope))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_by = order_by
                    .iter()
                    .map(|item| {
                        let expr = self.bind_expr(&item.expr, scope)?;
                        Ok(BoundOrderByExpr {
                            expr,
                            asc: item.asc,
                        })
                    })
                    .collect::<Result<Vec<_>, RustqlError>>()?;
                let data_type = window_function_type(function, &args);
                Ok(BoundExpr {
                    kind: BoundExprKind::WindowFunction {
                        function: function.clone(),
                        args: args.clone(),
                        partition_by: partition_by.clone(),
                        order_by: order_by.clone(),
                        frame: frame.clone(),
                    },
                    data_type,
                    nullable: true,
                    expr: Expression::WindowFunction {
                        function: function.clone(),
                        args: args.into_iter().map(|arg| arg.expr).collect(),
                        partition_by: partition_by.into_iter().map(|expr| expr.expr).collect(),
                        order_by: order_by
                            .into_iter()
                            .map(|item| OrderByExpr {
                                expr: item.expr.expr,
                                asc: item.asc,
                            })
                            .collect(),
                        frame: frame.clone(),
                    },
                })
            }
            Expression::Cast { expr, data_type } => {
                let bound = self.bind_expr(expr, scope)?;
                Ok(BoundExpr {
                    kind: BoundExprKind::Cast {
                        expr: Box::new(bound.clone()),
                        data_type: data_type.clone(),
                    },
                    data_type: BoundType::Known(data_type.clone()),
                    nullable: bound.nullable,
                    expr: Expression::Cast {
                        expr: Box::new(bound.expr),
                        data_type: data_type.clone(),
                    },
                })
            }
            Expression::IsDistinctFrom { left, right, not } => {
                let left = self.bind_expr(left, scope)?;
                let right = self.bind_expr(right, scope)?;
                ensure_comparable(&left.data_type, &right.data_type, "IS DISTINCT FROM")?;
                Ok(BoundExpr {
                    kind: BoundExprKind::IsDistinctFrom {
                        left: Box::new(left.clone()),
                        right: Box::new(right.clone()),
                        not: *not,
                    },
                    data_type: BoundType::Known(DataType::Boolean),
                    nullable: false,
                    expr: Expression::IsDistinctFrom {
                        left: Box::new(left.expr),
                        right: Box::new(right.expr),
                        not: *not,
                    },
                })
            }
        }
    }

    fn bind_predicate_expr(
        &mut self,
        expr: &Expression,
        scope: &NameScope,
        context: &str,
    ) -> Result<BoundExpr, RustqlError> {
        let bound = self.bind_expr(expr, scope)?;
        ensure_boolean(&bound, context)?;
        Ok(bound)
    }

    fn bind_order_by_expr(
        &mut self,
        expr: &Expression,
        scope: &NameScope,
        aliases: &[(String, BoundExpr)],
    ) -> Result<BoundExpr, RustqlError> {
        match expr {
            Expression::Column(name) => {
                if let Some((_, target)) = aliases.iter().find(|(alias, _)| alias == name) {
                    return Ok(BoundExpr {
                        kind: BoundExprKind::Alias { name: name.clone() },
                        data_type: target.data_type.clone(),
                        nullable: target.nullable,
                        expr: expr.clone(),
                    });
                }
                self.bind_expr(expr, scope)
            }
            Expression::Value(Value::Integer(_)) => self.bind_expr(expr, scope),
            Expression::BinaryOp { left, op, right } => {
                if matches!(op, BinaryOperator::Between) {
                    return self.bind_expr(expr, scope);
                }
                let left = self.bind_order_by_expr(left, scope, aliases)?;
                let right = self.bind_order_by_expr(right, scope, aliases)?;
                let data_type = self.bind_binary_type(op, &left, &right)?;
                Ok(BoundExpr {
                    kind: BoundExprKind::BinaryOp {
                        left: Box::new(left.clone()),
                        op: op.clone(),
                        right: Box::new(right.clone()),
                    },
                    data_type,
                    nullable: left.nullable || right.nullable,
                    expr: Expression::BinaryOp {
                        left: Box::new(left.expr),
                        op: op.clone(),
                        right: Box::new(right.expr),
                    },
                })
            }
            Expression::UnaryOp { op, expr } => {
                let expr = self.bind_order_by_expr(expr, scope, aliases)?;
                let data_type = match op {
                    UnaryOperator::Not => {
                        ensure_boolean(&expr, "ORDER BY expression")?;
                        BoundType::Known(DataType::Boolean)
                    }
                    UnaryOperator::Minus => {
                        ensure_numeric(&expr, "ORDER BY expression")?;
                        expr.data_type.clone()
                    }
                };
                Ok(BoundExpr {
                    kind: BoundExprKind::UnaryOp {
                        op: op.clone(),
                        expr: Box::new(expr.clone()),
                    },
                    data_type,
                    nullable: expr.nullable,
                    expr: Expression::UnaryOp {
                        op: op.clone(),
                        expr: Box::new(expr.expr),
                    },
                })
            }
            Expression::ScalarFunction { name, args } => {
                let args = args
                    .iter()
                    .map(|arg| self.bind_order_by_expr(arg, scope, aliases))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(BoundExpr {
                    kind: BoundExprKind::ScalarFunction {
                        name: name.clone(),
                        args: args.clone(),
                    },
                    data_type: scalar_function_type(name, &args),
                    nullable: true,
                    expr: Expression::ScalarFunction {
                        name: name.clone(),
                        args: args.into_iter().map(|arg| arg.expr).collect(),
                    },
                })
            }
            Expression::Cast { expr, data_type } => {
                let expr = self.bind_order_by_expr(expr, scope, aliases)?;
                Ok(BoundExpr {
                    kind: BoundExprKind::Cast {
                        expr: Box::new(expr.clone()),
                        data_type: data_type.clone(),
                    },
                    data_type: BoundType::Known(data_type.clone()),
                    nullable: expr.nullable,
                    expr: Expression::Cast {
                        expr: Box::new(expr.expr),
                        data_type: data_type.clone(),
                    },
                })
            }
            _ => self.bind_expr(expr, scope),
        }
    }

    fn bind_binary_type(
        &self,
        op: &BinaryOperator,
        left: &BoundExpr,
        right: &BoundExpr,
    ) -> Result<BoundType, RustqlError> {
        match op {
            BinaryOperator::And | BinaryOperator::Or => {
                ensure_boolean(left, "Boolean operator left operand")?;
                ensure_boolean(right, "Boolean operator right operand")?;
                Ok(BoundType::Known(DataType::Boolean))
            }
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide => {
                ensure_numeric(left, "Arithmetic expression")?;
                ensure_numeric(right, "Arithmetic expression")?;
                Ok(common_numeric_type(&left.data_type, &right.data_type))
            }
            BinaryOperator::Concat => Ok(BoundType::Known(DataType::Text)),
            BinaryOperator::Like | BinaryOperator::ILike => {
                ensure_text(left, "LIKE operator")?;
                ensure_text(right, "LIKE operator")?;
                Ok(BoundType::Known(DataType::Boolean))
            }
            BinaryOperator::Between => {
                ensure_comparable(&left.data_type, &right.data_type, "BETWEEN")?;
                Ok(BoundType::Known(DataType::Boolean))
            }
            BinaryOperator::In => {
                ensure_comparable(&left.data_type, &right.data_type, "IN")?;
                Ok(BoundType::Known(DataType::Boolean))
            }
            BinaryOperator::Equal
            | BinaryOperator::NotEqual
            | BinaryOperator::LessThan
            | BinaryOperator::LessThanOrEqual
            | BinaryOperator::GreaterThan
            | BinaryOperator::GreaterThanOrEqual => {
                ensure_comparable(&left.data_type, &right.data_type, "Comparison")?;
                Ok(BoundType::Known(DataType::Boolean))
            }
        }
    }

    fn bind_aggregate(
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

    fn bind_group_by(
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

    fn resolve_column(&self, name: &str, scope: &NameScope) -> Result<BoundColumnRef, RustqlError> {
        let local_matches = matching_columns(name, &scope.columns);
        match local_matches.len() {
            1 => return Ok(local_matches[0].clone()),
            n if n > 1 => return Err(RustqlError::AmbiguousColumn(name.to_string())),
            _ => {}
        }

        let outer_matches = matching_columns(name, &self.outer_columns);
        match outer_matches.len() {
            1 => {
                let mut column = outer_matches[0].clone();
                column.outer = true;
                Ok(column)
            }
            n if n > 1 => Err(RustqlError::AmbiguousColumn(name.to_string())),
            _ => Err(RustqlError::ColumnNotFound(name.to_string())),
        }
    }

    fn source_columns_for_name(
        &mut self,
        source_name: &str,
        relation_label: &str,
    ) -> Result<Vec<BoundColumnRef>, RustqlError> {
        if let Some(cte) = self
            .ctes
            .iter()
            .rev()
            .find(|cte| cte.name == source_name)
            .cloned()
        {
            let columns = self.cte_output_columns(&cte)?;
            return Ok(self.columns_for_relation(&columns, relation_label, true));
        }

        if let Some(table) = self.db.get_table(source_name) {
            return Ok(table
                .columns
                .iter()
                .map(|column| {
                    let column = self.semantic_column_definition(column);
                    bound_column(relation_label, &column, false)
                })
                .collect());
        }

        if let Some(view) = self.db.get_view(source_name) {
            let view_select = self.parse_view_query(&view.query_sql)?;
            let bound = self.bind_child_select(&view_select, Vec::new())?;
            return Ok(self.columns_for_relation(&bound.output_columns, relation_label, true));
        }

        Err(RustqlError::TableNotFound(source_name.to_string()))
    }

    fn cte_output_columns(&mut self, cte: &Cte) -> Result<Vec<ColumnDefinition>, RustqlError> {
        if self.binding_ctes.iter().any(|name| name == &cte.name) {
            return Ok(self.syntactic_output_columns(&cte.query));
        }

        let mut child = Binder {
            db: self.db,
            ctes: self.ctes.clone(),
            outer_columns: Vec::new(),
            binding_ctes: self.binding_ctes.clone(),
        };
        child.binding_ctes.push(cte.name.clone());
        child
            .bind_select(&cte.query)
            .map(|bound| bound.output_columns)
    }

    fn syntactic_output_columns(&self, stmt: &SelectStatement) -> Vec<ColumnDefinition> {
        let mut left = stmt.clone();
        if stmt.set_op.is_some() {
            left.set_op = None;
            return self.syntactic_output_columns(&left);
        }

        if matches!(left.columns.first(), Some(Column::All)) {
            return Vec::new();
        }

        left.columns
            .iter()
            .map(|column| {
                let name = match column {
                    Column::Named { name, alias } => alias.clone().unwrap_or_else(|| name.clone()),
                    Column::Expression { alias, .. } => {
                        alias.clone().unwrap_or_else(|| "<expression>".to_string())
                    }
                    Column::Function(aggregate) => format_aggregate_header(aggregate),
                    Column::Subquery(_) => "<subquery>".to_string(),
                    Column::All => "*".to_string(),
                };
                column_definition(name, syntactic_column_type(column), true)
            })
            .collect()
    }

    fn columns_for_relation(
        &self,
        columns: &[ColumnDefinition],
        relation_label: &str,
        use_unqualified_output_names: bool,
    ) -> Vec<BoundColumnRef> {
        columns
            .iter()
            .map(|column| {
                let mut column = column.clone();
                if use_unqualified_output_names {
                    column.name = unqualified_column_name(&column.name).to_string();
                }
                bound_column(relation_label, &column, false)
            })
            .collect()
    }

    fn semantic_column_definition(&self, column: &ColumnDefinition) -> ColumnDefinition {
        let Some((relation, name)) = column.name.split_once('.') else {
            return column.clone();
        };
        let Some(table) = self.db.get_table(relation) else {
            return column.clone();
        };
        let Some(source_column) = table
            .columns
            .iter()
            .find(|candidate| candidate.name == name)
        else {
            return column.clone();
        };

        let mut semantic = column.clone();
        semantic.data_type = source_column.data_type.clone();
        semantic.nullable = source_column.nullable;
        semantic
    }

    fn bind_values_source(
        &self,
        rows: &[Vec<Expression>],
        alias: &str,
        column_aliases: &[String],
    ) -> Result<Vec<BoundColumnRef>, RustqlError> {
        let width = rows.first().map_or(0, Vec::len);
        for row in rows {
            if row.len() != width {
                return Err(RustqlError::TypeMismatch(format!(
                    "VALUES rows must all have the same number of columns: expected {}, got {}",
                    width,
                    row.len()
                )));
            }
        }
        Ok((0..width)
            .map(|idx| {
                let data_type = rows
                    .iter()
                    .filter_map(|row| row.get(idx))
                    .filter_map(constant_expression_type)
                    .next()
                    .unwrap_or(DataType::Text);
                let column = column_definition(
                    column_aliases
                        .get(idx)
                        .cloned()
                        .unwrap_or_else(|| format!("column{}", idx + 1)),
                    data_type,
                    true,
                );
                bound_column(alias, &column, false)
            })
            .collect())
    }

    fn bind_table_function(
        &mut self,
        function: &mut TableFunction,
    ) -> Result<Vec<BoundColumnRef>, RustqlError> {
        let empty_scope = NameScope::default();
        for arg in &function.args {
            self.bind_expr(arg, &empty_scope)?;
        }

        match function.name.as_str() {
            "generate_series" => {
                let relation_label = function.alias.as_deref().unwrap_or(&function.name);
                let column = column_definition(
                    function
                        .alias
                        .clone()
                        .unwrap_or_else(|| "generate_series".to_string()),
                    DataType::Integer,
                    false,
                );
                Ok(vec![bound_column(relation_label, &column, false)])
            }
            other => Err(RustqlError::TypeMismatch(format!(
                "Unsupported table function '{}'",
                other
            ))),
        }
    }

    fn validate_join_using(
        &self,
        join: &Join,
        left_columns: &[BoundColumnRef],
        right_columns: &[BoundColumnRef],
    ) -> Result<(), RustqlError> {
        if let Some(using_columns) = join.using_columns.as_ref() {
            for column in using_columns {
                if !left_columns
                    .iter()
                    .any(|candidate| candidate.name == *column)
                    || !right_columns
                        .iter()
                        .any(|candidate| candidate.name == *column)
                {
                    return Err(RustqlError::ColumnNotFound(column.clone()));
                }
            }
        }
        Ok(())
    }

    fn star_output_columns(
        &self,
        stmt: &SelectStatement,
        scope: &NameScope,
    ) -> Vec<ColumnDefinition> {
        let qualify = !stmt.joins.is_empty();
        scope
            .columns
            .iter()
            .map(|column| {
                column_definition(
                    if qualify {
                        column.qualified_name.clone()
                    } else {
                        column.name.clone()
                    },
                    column.data_type.clone(),
                    column.nullable,
                )
            })
            .collect()
    }

    fn bind_child_select(
        &self,
        stmt: &SelectStatement,
        outer_columns: Vec<BoundColumnRef>,
    ) -> Result<BoundSelectStatement, RustqlError> {
        let mut child = self.child_binder(outer_columns);
        child.bind_select(stmt)
    }

    fn bind_correlated_select(
        &self,
        stmt: &SelectStatement,
        scope: &NameScope,
    ) -> Result<BoundSelectStatement, RustqlError> {
        self.bind_child_select(stmt, self.outer_for_child(&scope.columns))
    }

    fn child_binder(&self, outer_columns: Vec<BoundColumnRef>) -> Binder<'a> {
        Binder {
            db: self.db,
            ctes: self.ctes.clone(),
            outer_columns,
            binding_ctes: self.binding_ctes.clone(),
        }
    }

    fn outer_for_child(&self, local_columns: &[BoundColumnRef]) -> Vec<BoundColumnRef> {
        let mut outer = local_columns.to_vec();
        outer.extend(self.outer_columns.clone());
        outer
    }

    fn parse_view_query(&self, query_sql: &str) -> Result<SelectStatement, RustqlError> {
        let tokens = crate::lexer::tokenize(query_sql)?;
        match crate::parser::parse(tokens)? {
            Statement::Select(select) => Ok(select),
            _ => Err(RustqlError::TypeMismatch(
                "View definition is not a SELECT statement".to_string(),
            )),
        }
    }

    fn bind_create_table(
        &mut self,
        mut stmt: CreateTableStatement,
    ) -> Result<CreateTableStatement, RustqlError> {
        if let Some(query) = stmt.as_query.as_ref() {
            let bound = self.bind_select(query)?;
            stmt.as_query = Some(Box::new(bound.statement));
        }
        Ok(stmt)
    }

    fn bind_insert(&mut self, mut stmt: InsertStatement) -> Result<InsertStatement, RustqlError> {
        let table = self
            .db
            .get_table(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;
        let table_columns = table.columns.clone();

        let target_width = if let Some(columns) = stmt.columns.as_ref() {
            for column in columns {
                if !table_columns
                    .iter()
                    .any(|candidate| candidate.name == *column)
                {
                    return Err(RustqlError::ColumnNotFound(format!(
                        "{} (table: {})",
                        column, stmt.table
                    )));
                }
            }
            columns.len()
        } else {
            table_columns.len()
        };

        for values in &stmt.values {
            if values.len() != target_width {
                return Err(RustqlError::TypeMismatch(format!(
                    "Column count mismatch: expected {}, got {}",
                    target_width,
                    values.len()
                )));
            }
        }

        if let Some(query) = stmt.source_query.as_ref() {
            let bound = self.bind_select(query)?;
            if bound.output_columns.len() != target_width {
                return Err(RustqlError::TypeMismatch(format!(
                    "Column count mismatch: expected {}, got {}",
                    target_width,
                    bound.output_columns.len()
                )));
            }
            stmt.source_query = Some(Box::new(bound.statement));
        }

        if let Some(conflict) = stmt.on_conflict.as_ref() {
            for column in &conflict.columns {
                if !table_columns
                    .iter()
                    .any(|candidate| candidate.name == *column)
                {
                    return Err(RustqlError::ColumnNotFound(column.clone()));
                }
            }
        }

        let scope = NameScope {
            columns: table_columns
                .iter()
                .map(|column| bound_column(&stmt.table, column, false))
                .collect(),
        };
        if let Some(conflict) = stmt.on_conflict.as_mut()
            && let OnConflictAction::DoUpdate { assignments } = &mut conflict.action
        {
            self.bind_assignments(assignments, &table_columns, &scope)?;
        }

        if let Some(returning) = stmt.returning.as_mut() {
            self.bind_returning(returning, &scope)?;
        }

        Ok(stmt)
    }

    fn bind_update(&mut self, mut stmt: UpdateStatement) -> Result<UpdateStatement, RustqlError> {
        let table = self
            .db
            .get_table(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;
        let table_columns = table.columns.clone();
        let mut scope = NameScope {
            columns: table_columns
                .iter()
                .map(|column| bound_column(&stmt.table, column, false))
                .collect(),
        };

        if let Some(update_from) = stmt.from.as_ref() {
            scope
                .columns
                .extend(self.source_columns_for_name(&update_from.table, &update_from.table)?);
        }

        self.bind_assignments(&mut stmt.assignments, &table_columns, &scope)?;
        if let Some(where_clause) = stmt.where_clause.as_ref() {
            let bound = self.bind_predicate_expr(where_clause, &scope, "WHERE clause")?;
            stmt.where_clause = Some(bound.expr);
        }
        if let Some(returning) = stmt.returning.as_mut() {
            self.bind_returning(returning, &scope)?;
        }
        Ok(stmt)
    }

    fn bind_delete(&mut self, mut stmt: DeleteStatement) -> Result<DeleteStatement, RustqlError> {
        let table = self
            .db
            .get_table(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;
        let mut scope = NameScope {
            columns: table
                .columns
                .iter()
                .map(|column| bound_column(&stmt.table, column, false))
                .collect(),
        };

        if let Some(using) = stmt.using.as_ref() {
            let relation = using.alias.as_deref().unwrap_or(&using.table);
            scope
                .columns
                .extend(self.source_columns_for_name(&using.table, relation)?);
        }

        if let Some(where_clause) = stmt.where_clause.as_ref() {
            let bound = self.bind_predicate_expr(where_clause, &scope, "WHERE clause")?;
            stmt.where_clause = Some(bound.expr);
        }
        if let Some(returning) = stmt.returning.as_mut() {
            self.bind_returning(returning, &scope)?;
        }
        Ok(stmt)
    }

    fn bind_create_index(
        &mut self,
        mut stmt: CreateIndexStatement,
    ) -> Result<CreateIndexStatement, RustqlError> {
        let table = self
            .db
            .get_table(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;
        if stmt.columns.is_empty() {
            return Err(RustqlError::TypeMismatch(
                "CREATE INDEX requires at least one column".to_string(),
            ));
        }
        for column in &stmt.columns {
            if !table
                .columns
                .iter()
                .any(|candidate| candidate.name == *column)
            {
                return Err(RustqlError::ColumnNotFound(column.clone()));
            }
        }
        let scope = NameScope {
            columns: table
                .columns
                .iter()
                .map(|column| bound_column(&stmt.table, column, false))
                .collect(),
        };
        if let Some(where_clause) = stmt.where_clause.as_ref() {
            let bound = self.bind_predicate_expr(where_clause, &scope, "index WHERE clause")?;
            stmt.where_clause = Some(bound.expr);
        }
        Ok(stmt)
    }

    fn bind_merge(&mut self, mut stmt: MergeStatement) -> Result<MergeStatement, RustqlError> {
        let target = self
            .db
            .get_table(&stmt.target_table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.target_table.clone()))?;
        let target_columns = target.columns.clone();
        let mut scope = NameScope {
            columns: target_columns
                .iter()
                .map(|column| bound_column(&stmt.target_table, column, false))
                .collect(),
        };

        let source_label = match &mut stmt.source {
            MergeSource::Table { name, alias } => {
                let label = alias.as_deref().unwrap_or(name).to_string();
                scope
                    .columns
                    .extend(self.source_columns_for_name(name, &label)?);
                label
            }
            MergeSource::Subquery { query, alias } => {
                let bound = self.bind_child_select(query, Vec::new())?;
                **query = bound.statement;
                scope
                    .columns
                    .extend(self.columns_for_relation(&bound.output_columns, alias, true));
                alias.clone()
            }
        };
        let _ = source_label;

        let bound_on = self.bind_predicate_expr(&stmt.on_condition, &scope, "MERGE ON clause")?;
        stmt.on_condition = bound_on.expr;

        for clause in &mut stmt.when_clauses {
            match clause {
                MergeWhenClause::Matched { condition, action } => {
                    if let Some(condition_expr) = condition.as_mut() {
                        let bound = self.bind_predicate_expr(
                            condition_expr,
                            &scope,
                            "MERGE WHEN condition",
                        )?;
                        *condition_expr = bound.expr;
                    }
                    if let MergeMatchedAction::Update { assignments } = action {
                        self.bind_assignments(assignments, &target_columns, &scope)?;
                    }
                }
                MergeWhenClause::NotMatched { condition, action } => {
                    if let Some(condition_expr) = condition.as_mut() {
                        let bound = self.bind_predicate_expr(
                            condition_expr,
                            &scope,
                            "MERGE WHEN condition",
                        )?;
                        *condition_expr = bound.expr;
                    }
                    match action {
                        MergeNotMatchedAction::Insert { columns, values } => {
                            if let Some(columns) = columns {
                                for column in columns.iter() {
                                    if !target_columns
                                        .iter()
                                        .any(|candidate| candidate.name == *column)
                                    {
                                        return Err(RustqlError::ColumnNotFound(column.clone()));
                                    }
                                }
                                if values.len() != columns.len() {
                                    return Err(RustqlError::TypeMismatch(format!(
                                        "Column count mismatch: expected {}, got {}",
                                        columns.len(),
                                        values.len()
                                    )));
                                }
                            } else if values.len() != target_columns.len() {
                                return Err(RustqlError::TypeMismatch(format!(
                                    "Column count mismatch: expected {}, got {}",
                                    target_columns.len(),
                                    values.len()
                                )));
                            }

                            for value in values {
                                let bound = self.bind_expr(value, &scope)?;
                                *value = bound.expr;
                            }
                        }
                    }
                }
            }
        }

        Ok(stmt)
    }

    fn bind_assignments(
        &mut self,
        assignments: &mut [Assignment],
        target_columns: &[ColumnDefinition],
        scope: &NameScope,
    ) -> Result<(), RustqlError> {
        for assignment in assignments {
            if !target_columns
                .iter()
                .any(|column| column.name == assignment.column)
            {
                return Err(RustqlError::ColumnNotFound(assignment.column.clone()));
            }
            let bound = self.bind_expr(&assignment.value, scope)?;
            assignment.value = bound.expr;
        }
        Ok(())
    }

    fn bind_returning(
        &mut self,
        returning: &mut [Column],
        scope: &NameScope,
    ) -> Result<(), RustqlError> {
        for column in returning {
            match column {
                Column::All => {}
                Column::Named { name, .. } => {
                    self.resolve_column(name, scope)?;
                }
                Column::Expression { expr, .. } => {
                    let bound = self.bind_expr(expr, scope)?;
                    *expr = bound.expr;
                }
                Column::Function(aggregate) => {
                    let bound = self.bind_aggregate(aggregate, scope)?;
                    *aggregate = aggregate_from_bound(&bound);
                }
                Column::Subquery(subquery) => {
                    let bound = self.bind_correlated_select(subquery, scope)?;
                    **subquery = bound.statement;
                }
            }
        }
        Ok(())
    }
}

fn group_by_from_bound(original: &GroupByClause, bound: &[BoundExpr]) -> GroupByClause {
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

fn aggregate_from_bound(bound: &BoundAggregateFunction) -> AggregateFunction {
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

fn ensure_single_column_subquery(
    subquery: &BoundSelectStatement,
    context: &str,
) -> Result<(), RustqlError> {
    if subquery.output_columns.len() != 1 {
        return Err(RustqlError::TypeMismatch(format!(
            "{} must return exactly one column",
            context
        )));
    }
    Ok(())
}

fn matching_columns(name: &str, columns: &[BoundColumnRef]) -> Vec<BoundColumnRef> {
    if let Some((relation, column)) = name.split_once('.') {
        columns
            .iter()
            .filter(|candidate| {
                candidate.name == name
                    || (candidate.relation.as_deref() == Some(relation)
                        && (candidate.name == column
                            || unqualified_column_name(&candidate.name) == column))
            })
            .cloned()
            .collect()
    } else {
        columns
            .iter()
            .filter(|candidate| {
                candidate.name == name || unqualified_column_name(&candidate.name) == name
            })
            .cloned()
            .collect()
    }
}

fn bound_column(relation: &str, column: &ColumnDefinition, outer: bool) -> BoundColumnRef {
    BoundColumnRef {
        relation: Some(relation.to_string()),
        name: column.name.clone(),
        qualified_name: format!("{}.{}", relation, column.name),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
        outer,
    }
}

fn column_ref_type(reference: &BoundColumnRef) -> BoundType {
    if reference
        .relation
        .as_deref()
        .is_some_and(|relation| relation.starts_with("__lateral_outer_"))
        || reference.name.starts_with("__outer_col_")
    {
        BoundType::Unknown
    } else {
        BoundType::Known(reference.data_type.clone())
    }
}

fn column_definition(name: String, data_type: DataType, nullable: bool) -> ColumnDefinition {
    ColumnDefinition {
        name,
        data_type,
        nullable,
        primary_key: false,
        unique: false,
        default_value: None,
        foreign_key: None,
        check: None,
        auto_increment: false,
        generated: None,
    }
}

fn value_type(value: &Value) -> BoundType {
    match value {
        Value::Null => BoundType::Unknown,
        Value::Integer(_) => BoundType::Known(DataType::Integer),
        Value::Float(_) => BoundType::Known(DataType::Float),
        Value::Text(_) => BoundType::Known(DataType::Text),
        Value::Boolean(_) => BoundType::Known(DataType::Boolean),
        Value::Date(_) => BoundType::Known(DataType::Date),
        Value::Time(_) => BoundType::Known(DataType::Time),
        Value::DateTime(_) => BoundType::Known(DataType::DateTime),
    }
}

fn constant_expression_type(expr: &Expression) -> Option<DataType> {
    if let Expression::Value(value) = expr
        && let BoundType::Known(data_type) = value_type(value)
    {
        return Some(data_type);
    }
    None
}

fn syntactic_column_type(column: &Column) -> DataType {
    match column {
        Column::Expression { expr, .. } => syntactic_expr_type(expr).unwrap_or(DataType::Text),
        Column::Function(aggregate) => match aggregate.function {
            AggregateFunctionType::Count => DataType::Integer,
            AggregateFunctionType::Avg
            | AggregateFunctionType::Stddev
            | AggregateFunctionType::Variance
            | AggregateFunctionType::Median
            | AggregateFunctionType::PercentileCont
            | AggregateFunctionType::PercentileDisc => DataType::Float,
            AggregateFunctionType::BoolAnd | AggregateFunctionType::BoolOr => DataType::Boolean,
            _ => DataType::Text,
        },
        _ => DataType::Text,
    }
}

fn syntactic_expr_type(expr: &Expression) -> Option<DataType> {
    match expr {
        Expression::Value(value) => match value_type(value) {
            BoundType::Known(data_type) => Some(data_type),
            BoundType::Unknown => None,
        },
        Expression::Cast { data_type, .. } => Some(data_type.clone()),
        Expression::BinaryOp { left, op, right } => match op {
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide => {
                let left = syntactic_expr_type(left)?;
                let right = syntactic_expr_type(right)?;
                if matches!(left, DataType::Float) || matches!(right, DataType::Float) {
                    Some(DataType::Float)
                } else if matches!(left, DataType::Integer) && matches!(right, DataType::Integer) {
                    Some(DataType::Integer)
                } else {
                    None
                }
            }
            BinaryOperator::Concat => Some(DataType::Text),
            BinaryOperator::And
            | BinaryOperator::Or
            | BinaryOperator::Equal
            | BinaryOperator::NotEqual
            | BinaryOperator::LessThan
            | BinaryOperator::LessThanOrEqual
            | BinaryOperator::GreaterThan
            | BinaryOperator::GreaterThanOrEqual
            | BinaryOperator::Like
            | BinaryOperator::ILike
            | BinaryOperator::Between
            | BinaryOperator::In => Some(DataType::Boolean),
        },
        Expression::UnaryOp { op, expr } => match op {
            UnaryOperator::Minus => syntactic_expr_type(expr),
            UnaryOperator::Not => Some(DataType::Boolean),
        },
        Expression::IsNull { .. }
        | Expression::Exists(_)
        | Expression::Any { .. }
        | Expression::All { .. }
        | Expression::IsDistinctFrom { .. }
        | Expression::In { .. } => Some(DataType::Boolean),
        Expression::ScalarFunction { name, .. } => scalar_function_type(name, &[])
            .known()
            .or(Some(DataType::Text)),
        _ => None,
    }
}

impl BoundType {
    fn known(self) -> Option<DataType> {
        match self {
            BoundType::Known(data_type) => Some(data_type),
            BoundType::Unknown => None,
        }
    }
}

fn bound_type_or_text(data_type: &BoundType) -> DataType {
    match data_type {
        BoundType::Known(data_type) => data_type.clone(),
        BoundType::Unknown => DataType::Text,
    }
}

fn aggregate_type(function: &AggregateFunctionType, expr: &BoundExpr) -> BoundType {
    match function {
        AggregateFunctionType::Count => BoundType::Known(DataType::Integer),
        AggregateFunctionType::Sum => match expr.data_type {
            BoundType::Known(DataType::Integer) => BoundType::Known(DataType::Integer),
            BoundType::Known(DataType::Float) => BoundType::Known(DataType::Float),
            _ => BoundType::Unknown,
        },
        AggregateFunctionType::Avg
        | AggregateFunctionType::Stddev
        | AggregateFunctionType::Variance
        | AggregateFunctionType::Median
        | AggregateFunctionType::PercentileCont
        | AggregateFunctionType::PercentileDisc => BoundType::Known(DataType::Float),
        AggregateFunctionType::Min | AggregateFunctionType::Max | AggregateFunctionType::Mode => {
            expr.data_type.clone()
        }
        AggregateFunctionType::GroupConcat => BoundType::Known(DataType::Text),
        AggregateFunctionType::BoolAnd | AggregateFunctionType::BoolOr => {
            BoundType::Known(DataType::Boolean)
        }
    }
}

fn scalar_function_type(name: &ScalarFunctionType, args: &[BoundExpr]) -> BoundType {
    match name {
        ScalarFunctionType::Upper
        | ScalarFunctionType::Lower
        | ScalarFunctionType::Substring
        | ScalarFunctionType::Trim
        | ScalarFunctionType::Replace
        | ScalarFunctionType::ConcatFn
        | ScalarFunctionType::Lpad
        | ScalarFunctionType::Rpad
        | ScalarFunctionType::LeftFn
        | ScalarFunctionType::RightFn
        | ScalarFunctionType::Reverse
        | ScalarFunctionType::Repeat
        | ScalarFunctionType::DateTrunc
        | ScalarFunctionType::Ltrim
        | ScalarFunctionType::Rtrim
        | ScalarFunctionType::Chr
        | ScalarFunctionType::Initcap
        | ScalarFunctionType::SplitPart
        | ScalarFunctionType::Translate
        | ScalarFunctionType::RegexpReplace => BoundType::Known(DataType::Text),
        ScalarFunctionType::Length
        | ScalarFunctionType::Position
        | ScalarFunctionType::Instr
        | ScalarFunctionType::Year
        | ScalarFunctionType::Month
        | ScalarFunctionType::Day
        | ScalarFunctionType::Datediff
        | ScalarFunctionType::Ascii
        | ScalarFunctionType::Week
        | ScalarFunctionType::DayOfWeek
        | ScalarFunctionType::Quarter
        | ScalarFunctionType::Gcd
        | ScalarFunctionType::Lcm => BoundType::Known(DataType::Integer),
        ScalarFunctionType::Now => BoundType::Known(DataType::DateTime),
        ScalarFunctionType::Random
        | ScalarFunctionType::Pi
        | ScalarFunctionType::Log
        | ScalarFunctionType::Exp
        | ScalarFunctionType::Sin
        | ScalarFunctionType::Cos
        | ScalarFunctionType::Tan
        | ScalarFunctionType::Asin
        | ScalarFunctionType::Acos
        | ScalarFunctionType::Atan
        | ScalarFunctionType::Atan2
        | ScalarFunctionType::Degrees
        | ScalarFunctionType::Radians
        | ScalarFunctionType::Log10
        | ScalarFunctionType::Log2
        | ScalarFunctionType::Cbrt
        | ScalarFunctionType::Sqrt
        | ScalarFunctionType::Power => BoundType::Known(DataType::Float),
        ScalarFunctionType::Coalesce
        | ScalarFunctionType::Nullif
        | ScalarFunctionType::Greatest
        | ScalarFunctionType::Least => args
            .iter()
            .map(|arg| arg.data_type.clone())
            .find(|data_type| !matches!(data_type, BoundType::Unknown))
            .unwrap_or(BoundType::Unknown),
        ScalarFunctionType::Abs
        | ScalarFunctionType::Round
        | ScalarFunctionType::Ceil
        | ScalarFunctionType::Floor
        | ScalarFunctionType::Mod
        | ScalarFunctionType::Sign
        | ScalarFunctionType::Trunc => args
            .first()
            .map(|arg| arg.data_type.clone())
            .unwrap_or(BoundType::Unknown),
        ScalarFunctionType::DateAdd => BoundType::Known(DataType::Date),
        ScalarFunctionType::Extract => BoundType::Known(DataType::Integer),
        ScalarFunctionType::RegexpMatch => BoundType::Known(DataType::Boolean),
    }
}

fn window_function_type(function: &WindowFunctionType, args: &[BoundExpr]) -> BoundType {
    match function {
        WindowFunctionType::RowNumber
        | WindowFunctionType::Rank
        | WindowFunctionType::DenseRank
        | WindowFunctionType::Ntile => BoundType::Known(DataType::Integer),
        WindowFunctionType::PercentRank | WindowFunctionType::CumeDist => {
            BoundType::Known(DataType::Float)
        }
        WindowFunctionType::Aggregate(function) => args
            .first()
            .map(|arg| aggregate_type(function, arg))
            .unwrap_or(BoundType::Unknown),
        WindowFunctionType::Lag
        | WindowFunctionType::Lead
        | WindowFunctionType::FirstValue
        | WindowFunctionType::LastValue
        | WindowFunctionType::NthValue => args
            .first()
            .map(|arg| arg.data_type.clone())
            .unwrap_or(BoundType::Unknown),
    }
}

fn ensure_boolean(expr: &BoundExpr, context: &str) -> Result<(), RustqlError> {
    if matches!(
        expr.data_type,
        BoundType::Known(DataType::Boolean) | BoundType::Unknown
    ) {
        Ok(())
    } else {
        Err(RustqlError::TypeMismatch(format!(
            "{} must evaluate to BOOLEAN",
            context
        )))
    }
}

fn ensure_numeric(expr: &BoundExpr, context: &str) -> Result<(), RustqlError> {
    match expr.data_type {
        BoundType::Known(DataType::Integer | DataType::Float) | BoundType::Unknown => Ok(()),
        _ => Err(RustqlError::TypeMismatch(format!(
            "{} requires numeric values",
            context
        ))),
    }
}

fn ensure_text(expr: &BoundExpr, context: &str) -> Result<(), RustqlError> {
    match expr.data_type {
        BoundType::Known(DataType::Text) | BoundType::Unknown => Ok(()),
        _ => Err(RustqlError::TypeMismatch(format!(
            "{} requires text values",
            context
        ))),
    }
}

fn ensure_comparable(
    left: &BoundType,
    right: &BoundType,
    context: &str,
) -> Result<(), RustqlError> {
    if types_comparable(left, right) {
        Ok(())
    } else {
        Err(RustqlError::TypeMismatch(format!(
            "{} operands have incompatible types: {} and {}",
            context,
            display_bound_type(left),
            display_bound_type(right)
        )))
    }
}

fn types_comparable(left: &BoundType, right: &BoundType) -> bool {
    match (left, right) {
        (BoundType::Unknown, _) | (_, BoundType::Unknown) => true,
        (BoundType::Known(left), BoundType::Known(right)) if left == right => true,
        (BoundType::Known(left), BoundType::Known(right)) => {
            is_numeric_type(left) && is_numeric_type(right)
        }
    }
}

fn common_numeric_type(left: &BoundType, right: &BoundType) -> BoundType {
    match (left, right) {
        (BoundType::Known(DataType::Float), _) | (_, BoundType::Known(DataType::Float)) => {
            BoundType::Known(DataType::Float)
        }
        (BoundType::Known(DataType::Integer), BoundType::Known(DataType::Integer)) => {
            BoundType::Known(DataType::Integer)
        }
        _ => BoundType::Unknown,
    }
}

fn common_type(left: &BoundType, right: &BoundType) -> BoundType {
    match (left, right) {
        (BoundType::Unknown, data_type) | (data_type, BoundType::Unknown) => data_type.clone(),
        (BoundType::Known(left), BoundType::Known(right)) if left == right => {
            BoundType::Known(left.clone())
        }
        _ if types_comparable(left, right) => common_numeric_type(left, right),
        _ => BoundType::Unknown,
    }
}

fn is_numeric_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Integer | DataType::Float)
}

fn display_bound_type(data_type: &BoundType) -> &'static str {
    match data_type {
        BoundType::Unknown => "UNKNOWN",
        BoundType::Known(DataType::Integer) => "INTEGER",
        BoundType::Known(DataType::Float) => "FLOAT",
        BoundType::Known(DataType::Text) => "TEXT",
        BoundType::Known(DataType::Boolean) => "BOOLEAN",
        BoundType::Known(DataType::Date) => "DATE",
        BoundType::Known(DataType::Time) => "TIME",
        BoundType::Known(DataType::DateTime) => "DATETIME",
    }
}

fn unqualified_column_name(name: &str) -> &str {
    name.split('.').next_back().unwrap_or(name)
}
