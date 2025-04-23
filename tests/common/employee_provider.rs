use std::collections::{HashMap, HashSet};
use std::{any::Any, sync::Arc};

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};

use datafusion::physical_plan::{empty::EmptyExec, union::UnionExec, ExecutionPlan};
use datafusion::scalar::ScalarValue;
use datafusion_index_provider::optimizer::try_combine_exprs_to_between;
use datafusion_index_provider::physical::joins::try_create_lookup_join;
use datafusion_index_provider::*;

use super::exec::{IndexJoinExec, IndexLookupExec};
use super::indexes::{AgeIndex, DepartmentIndex};

/// A simple in-memory table provider that stores employee data with an age index
#[derive(Debug)]
pub struct EmployeeTableProvider {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    age_index: AgeIndex,
    department_index: DepartmentIndex,
}

#[async_trait]
impl IndexProvider for EmployeeTableProvider {
    fn get_indexed_columns_names(&self) -> HashSet<String> {
        let mut columns = HashSet::new();
        columns.insert("age".to_string());
        columns.insert("department".to_string());
        columns
    }

    fn optimize_exprs(&self, exprs: &[Expr]) -> Result<Vec<Expr>> {
        // Group expressions by column
        let mut column_exprs: HashMap<String, Vec<&Expr>> = HashMap::new();
        let mut optimized = Vec::new();

        for expr in exprs {
            if let Expr::BinaryExpr(binary) = expr {
                if let (Expr::Column(col), Expr::Literal(_)) = (&*binary.left, &*binary.right) {
                    if self.get_indexed_columns_names().contains(&col.name) {
                        column_exprs.entry(col.name.clone()).or_default().push(expr);
                        continue;
                    }
                }
            }
            // If not a binary expression or not supported by index, keep as is
            optimized.push(expr.clone());
        }

        // Process expressions for each column
        for (col_name, col_exprs) in column_exprs {
            if col_exprs.len() > 1 {
                // Try to combine expressions into bounds
                if let Some(combined) = try_combine_exprs_to_between(&col_exprs, &col_name) {
                    optimized.extend(combined);
                } else {
                    // If we couldn't combine them, keep them as is
                    optimized.extend(col_exprs.into_iter().cloned());
                }
            } else {
                // For single expressions, keep them as is
                optimized.extend(col_exprs.into_iter().cloned());
            }
        }

        Ok(optimized)
    }

    fn create_index_lookup(&self, expr: &Expr) -> Result<Arc<dyn ExecutionPlan>> {
        log::debug!("Creating index lookup for expression: {:?}", expr);
        match expr {
            Expr::BinaryExpr(binary) => match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(col), Expr::Literal(value)) => {
                    let filtered_indices = match (col.name.as_str(), value) {
                        ("age", ScalarValue::Int32(Some(age))) => {
                            self.age_index.filter_rows(&binary.op, *age)
                        }
                        ("department", ScalarValue::Utf8(Some(dept))) => {
                            self.department_index.filter_rows(&binary.op, dept)
                        }
                        _ => return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty())))),
                    };

                    let index_schema = Arc::new(Schema::new(vec![Field::new(
                        "index",
                        DataType::UInt64,
                        false,
                    )]));

                    Ok(Arc::new(IndexLookupExec::new(
                        index_schema,
                        filtered_indices,
                    )))
                }
                (Expr::BinaryExpr(left), Expr::BinaryExpr(right)) if binary.op == Operator::Or => {
                    let left_expr = Expr::BinaryExpr(left.clone());
                    let right_expr = Expr::BinaryExpr(right.clone());

                    let left_exec = self.create_index_lookup(&left_expr)?;
                    let right_exec = self.create_index_lookup(&right_expr)?;

                    log::debug!("Combining OR expression with UnionExec");
                    Ok(Arc::new(UnionExec::new(vec![left_exec, right_exec])))
                }
                _ => Err(DataFusionError::Internal(format!(
                    "Unsupported expression structure within BinaryExpr: {:?}",
                    binary
                ))),
            },
            Expr::Between(between) => {
                if let Expr::Column(col) = between.expr.as_ref() {
                    if let (
                        Expr::Literal(ScalarValue::Int32(Some(low))),
                        Expr::Literal(ScalarValue::Int32(Some(high))),
                    ) = (between.low.as_ref(), between.high.as_ref())
                    {
                        if col.name == "age" {
                            let filtered_indices: Vec<u64> =
                                self.age_index.filter_rows_range(*low, *high);

                            let index_schema = Arc::new(Schema::new(vec![Field::new(
                                "index",
                                DataType::UInt64,
                                false,
                            )]));

                            return Ok(Arc::new(IndexLookupExec::new(
                                index_schema,
                                filtered_indices,
                            )));
                        }
                    }
                }
                Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))))
            }
            _ => Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty())))),
        }
    }

    fn create_index_join(
        &self,
        lookups: Vec<Arc<dyn ExecutionPlan>>,
        projection: Option<&Vec<usize>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match lookups.len() {
            0 => Err(DataFusionError::Internal("No lookups provided".to_string())),
            1 => {
                // Handle single column case
                Ok(Arc::new(IndexJoinExec::new(
                    lookups[0].clone(),
                    self.batches.clone(),
                    projection.cloned(),
                    self.schema.clone(),
                )))
            }
            2 => {
                // Handle two columns case with HashJoin or SortMergeJoin
                let left = lookups[0].clone();
                let right = lookups[1].clone();

                let join_exec = try_create_lookup_join(left, right)?;

                // Join with the actual data using IndexJoinExec
                Ok(Arc::new(IndexJoinExec::new(
                    join_exec,
                    self.batches.clone(),
                    projection.cloned(),
                    self.schema.clone(),
                )))
            }
            _ => Err(DataFusionError::NotImplemented(
                "Joining more than 2 indices is not supported".to_string(),
            )),
        }
    }
}

impl Default for EmployeeTableProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl EmployeeTableProvider {
    pub fn new() -> Self {
        // Define schema: id, name, department, age
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
            Field::new("department", DataType::Utf8, false),
        ]));

        // Sample employee data
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
        let age_array = Int32Array::from(vec![25, 30, 35, 28, 32]);
        let department_array = StringArray::from(vec![
            "Engineering",
            "Sales",
            "Marketing",
            "Engineering",
            "Sales",
        ]);

        // Create the age index
        let age_index = AgeIndex::new(&age_array);

        // Create the department index
        let department_index = DepartmentIndex::new(&department_array);

        // Create record batch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(age_array.clone()),
                Arc::new(department_array.clone()),
            ],
        )
        .unwrap();

        EmployeeTableProvider {
            schema,
            batches: vec![batch],
            age_index,
            department_index,
        }
    }
}

#[async_trait]
impl TableProvider for EmployeeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        log::debug!(
            "EmployeeTableProvider::scan. projection: {:?}, filters: {:?}, limit: {:?}",
            projection,
            filters,
            limit
        );

        // Step 1: Analyze Filters and Optimize
        let mut index_filters = Vec::new();
        let mut remaining_filters = Vec::new();
        let indexed_columns = self.get_indexed_columns_names();

        for filter in filters {
            let mut pushed_down = false;
            if let Expr::BinaryExpr(binary) = filter {
                if let Expr::Column(col) = &*binary.left {
                    if indexed_columns.contains(&col.name) {
                        index_filters.push(filter.clone());
                        pushed_down = true;
                    }
                }
            }
            // Add other indexable patterns here (e.g., BETWEEN on indexed columns)
            // ... TBD ...

            if !pushed_down {
                remaining_filters.push(filter.clone());
            }
        }

        // --- Optimization logic adapted from `optimize_exprs` ---
        let mut optimized_index_filters = Vec::new();
        let mut column_exprs: HashMap<String, Vec<Expr>> = HashMap::new();

        for expr in &index_filters {
            // Iterate over collected index filters
            if let Expr::BinaryExpr(binary) = expr {
                if let (Expr::Column(col), Expr::Literal(_)) = (&*binary.left, &*binary.right) {
                    if indexed_columns.contains(&col.name) {
                        // Group by column name
                        column_exprs
                            .entry(col.name.clone())
                            .or_default()
                            .push(expr.clone());
                        continue; // Skip adding to optimized_index_filters directly
                    }
                }
            }
            // If not a binary expression on an indexed column, or can't be grouped, add directly
            optimized_index_filters.push(expr.clone());
        }

        // Process grouped expressions for optimization (e.g., BETWEEN)
        for (col_name, col_exprs) in column_exprs {
            if col_exprs.len() > 1 {
                // Try to combine expressions into bounds
                let col_exprs_refs: Vec<&Expr> = col_exprs.iter().collect(); // Collect references
                if let Some(combined) =
                    try_combine_exprs_to_between(col_exprs_refs.as_slice(), &col_name)
                {
                    // Pass as slice
                    optimized_index_filters.extend(combined);
                } else {
                    // If we couldn't combine them, keep them as is
                    optimized_index_filters.extend(col_exprs);
                }
            } else {
                // For single expressions, keep them as is
                optimized_index_filters.extend(col_exprs);
            }
        }
        // --- End Optimization logic ---

        log::debug!("Optimized index filters: {:?}", optimized_index_filters);
        log::debug!("Remaining filters: {:?}", remaining_filters);

        // TODO: Step 2: Build Index Scan Plan(s) (adapt create_index_lookup -> IndexScanExec)
        // - Use `optimized_index_filters` here
        // Temporary: Return empty plan until implemented
        let base_schema = self.schema(); // Or apply projection
        Ok(Arc::new(EmptyExec::new(base_schema)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.supports_index_filters_pushdown(filters)
    }
}
