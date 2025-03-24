use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, JoinType, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::memory::LazyMemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use datafusion_index_provider::optimizer::try_combine_exprs_to_between;
use datafusion_index_provider::*;
use parking_lot::RwLock;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::catalog::Session;

use super::exec::{IndexJoinExec, IndexLookupExec};
use super::indexes::{AgeIndex, DepartmentIndex, NotSoLazyBatchGenerator};

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
    fn get_indexed_columns(&self) -> HashMap<String, IndexedColumn> {
        let mut columns = HashMap::new();
        columns.insert(
            "age".to_string(),
            IndexedColumn {
                name: "age".to_string(),
                supported_operators: vec![
                    Operator::Eq,
                    Operator::Lt,
                    Operator::LtEq,
                    Operator::Gt,
                    Operator::GtEq,
                ],
            },
        );
        columns.insert(
            "department".to_string(),
            IndexedColumn {
                name: "department".to_string(),
                supported_operators: vec![Operator::Eq],
            },
        );
        columns
    }

    fn optimize_exprs(&self, exprs: &[Expr]) -> Result<Vec<Expr>> {
        // Group expressions by column
        let mut column_exprs: HashMap<String, Vec<&Expr>> = HashMap::new();
        let mut optimized = Vec::new();

        for expr in exprs {
            if let Expr::BinaryExpr(binary) = expr {
                if let (Expr::Column(col), Expr::Literal(_)) = (&*binary.left, &*binary.right) {
                    if self.supports_operator(&col.name, &binary.op) {
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

    fn create_index_lookup(&self, expr: &Expr) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        match expr {
            Expr::BinaryExpr(binary) => {
                if let (Expr::Column(col), Expr::Literal(value)) = (&*binary.left, &*binary.right) {
                    if !self.supports_operator(&col.name, &binary.op) {
                        return Ok(None);
                    }

                    let filtered_indices = match (col.name.as_str(), value) {
                        ("age", ScalarValue::Int32(Some(age))) => {
                            self.age_index.filter_rows(&binary.op, *age)
                        }
                        ("department", ScalarValue::Utf8(Some(dept))) => {
                            self.department_index.filter_rows(&binary.op, dept)
                        }
                        _ => return Ok(None),
                    };

                    let index_schema = Arc::new(Schema::new(vec![Field::new(
                        "index",
                        DataType::UInt64,
                        false,
                    )]));

                    Ok(Some(Arc::new(IndexLookupExec::new(
                        index_schema,
                        filtered_indices,
                    ))))
                } else {
                    Ok(None)
                }
            }
            Expr::Between(between) => {
                if let Expr::Column(col) = between.expr.as_ref() {
                    if let (
                        Expr::Literal(ScalarValue::Int32(Some(low))),
                        Expr::Literal(ScalarValue::Int32(Some(high))),
                    ) = (between.low.as_ref(), between.high.as_ref())
                    {
                        if col.name == "age" {
                            let filtered_indices: Vec<usize> =
                                self.age_index.filter_rows_range(*low, *high);

                            let index_schema = Arc::new(Schema::new(vec![Field::new(
                                "index",
                                DataType::UInt64,
                                false,
                            )]));

                            return Ok(Some(Arc::new(IndexLookupExec::new(
                                index_schema,
                                filtered_indices,
                            ))));
                        }
                    }
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn create_index_join(
        &self,
        lookups: Vec<Arc<dyn ExecutionPlan>>,
        projection: Option<&Vec<usize>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match lookups.len() {
            0 => {
                let data = self.batches.clone();
                let schema = self.schema.clone();

                let generator = Arc::new(RwLock::new(NotSoLazyBatchGenerator { data }));

                Ok(Arc::new(LazyMemoryExec::try_new(schema, vec![generator])?))
            }
            1 => {
                // Single index - use IndexJoinExec
                Ok(Arc::new(IndexJoinExec::new(
                    lookups[0].clone(),
                    self.batches.clone(),
                    projection.cloned(),
                    self.schema.clone(),
                )))
            }
            2 => {
                // Handle multiple columns case with HashJoin
                let left = lookups[0].clone();
                let right = lookups[1].clone();

                // Join on the index column which is present in both index results
                let left_col = Arc::new(PhysicalColumn::new("index", 0)) as Arc<dyn PhysicalExpr>;
                let right_col = Arc::new(PhysicalColumn::new("index", 0)) as Arc<dyn PhysicalExpr>;
                let join_on = vec![(left_col, right_col)];

                // Create HashJoinExec to join the index results
                let join_exec = Arc::new(HashJoinExec::try_new(
                    left,
                    right,
                    join_on,
                    None,
                    &JoinType::Inner,
                    None, // No filter columns
                    PartitionMode::CollectLeft,
                    false,
                )?);

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
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // First optimize the expressions
        let optimized_filters = self.optimize_exprs(filters)?;

        log::debug!("Optimized filters: {:?}", optimized_filters);

        // Create index lookups for each optimized filter
        let mut lookups = Vec::new();
        for filter in optimized_filters {
            if let Some(lookup) = self.create_index_lookup(&filter)? {
                lookups.push(lookup);
            }
        }

        // Create the appropriate join plan based on number of lookups
        self.create_index_join(lookups, projection)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        log::debug!("Checking filters: {:?}", filters);
        let mut pushdown = Vec::with_capacity(filters.len());
        for filter in filters {
            if let Expr::BinaryExpr(expr) = *filter {
                if let (Expr::Column(col), Expr::Literal(_)) = (&*expr.left, &*expr.right) {
                    if self.supports_operator(&col.name, &expr.op) {
                        pushdown.push(TableProviderFilterPushDown::Exact);
                        continue;
                    }
                }
            }
            pushdown.push(TableProviderFilterPushDown::Unsupported);
        }
        Ok(pushdown)
    }
}
