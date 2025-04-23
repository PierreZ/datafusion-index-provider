use std::collections::HashSet;
use std::{any::Any, sync::Arc};

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{empty::EmptyExec, ExecutionPlan};
use datafusion::scalar::ScalarValue;
use datafusion_index_provider::physical::indexes::index::Index;
use datafusion_index_provider::physical::indexes::scan::index_scan_schema;
use datafusion_index_provider::physical::joins::try_create_lookup_join;
use datafusion_index_provider::provider::IndexedTableProvider;

use crate::common::age_index::AgeIndex;
use crate::common::department_index::DepartmentIndex;
use crate::common::exec::{IndexJoinExec, IndexLookupExec};

/// A simple in-memory table provider that stores employee data with an age index
#[derive(Debug)]
pub struct EmployeeTableProvider {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    age_index: Arc<AgeIndex>,
    department_index: Arc<DepartmentIndex>,
    indexes: Vec<Arc<dyn Index>>,
}

#[async_trait]
impl IndexedTableProvider for EmployeeTableProvider {
    fn indexes(&self) -> Result<Vec<Arc<dyn Index>>> {
        Ok(self.indexes.clone())
    }

    fn get_indexed_columns_names(&self) -> Result<HashSet<String>> {
        let mut columns = HashSet::new();
        columns.insert("age".to_string());
        columns.insert("department".to_string());
        Ok(columns)
    }

    fn create_index_scan_exec_for_expr(&self, expr: &Expr) -> Result<Arc<dyn ExecutionPlan>> {
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

                    let index_schema = index_scan_schema(DataType::UInt64);

                    Ok(Arc::new(IndexLookupExec::new(
                        index_schema,
                        filtered_indices,
                        false, // Not sorted
                    )))
                }
                (Expr::BinaryExpr(left), Expr::BinaryExpr(right)) if binary.op == Operator::Or => {
                    let left_expr = Expr::BinaryExpr(left.clone());
                    let right_expr = Expr::BinaryExpr(right.clone());

                    let left_exec = self.create_index_scan_exec_for_expr(&left_expr)?;
                    let right_exec = self.create_index_scan_exec_for_expr(&right_expr)?;

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

                            let index_schema = index_scan_schema(DataType::UInt64);

                            return Ok(Arc::new(IndexLookupExec::new(
                                index_schema,
                                filtered_indices,
                                false, // Not sorted
                            )));
                        }
                    }
                }
                Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))))
            }
            _ => Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty())))),
        }
    }

    fn merge_indexes_streams(
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

    // `find_suitable_indexes` uses the default implementation provided by the trait for now.
}

impl Default for EmployeeTableProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl EmployeeTableProvider {
    pub fn new() -> Self {
        // Define schema: id, name, age, department
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
        let age_index = Arc::new(AgeIndex::new(&age_array));

        // Create the department index
        let department_index = Arc::new(DepartmentIndex::new(&department_array));

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
            age_index: age_index.clone(),
            department_index: department_index.clone(),
            indexes: vec![
                age_index as Arc<dyn Index>,
                department_index as Arc<dyn Index>,
            ],
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
        let (optimized_index_filters, _remaining_filters) =
            self.analyze_and_optimize_filters(filters)?;
        // TODO: Use remaining_filters later, perhaps in FilterExec after the index scan/join

        log::debug!("Optimized index filters: {:?}", optimized_index_filters);

        // Step 2: Build Index Scan Plan(s)
        let index_scan_plans: Vec<Arc<dyn ExecutionPlan>> = optimized_index_filters
            .iter()
            .map(|filter| self.create_index_scan_exec_for_expr(filter))
            .collect::<Result<Vec<_>>>()?; // Collect results, propagating errors

        log::debug!("Created {} index scan plans", index_scan_plans.len());

        // Step 3: Build Index Join Plan or Fallback
        if index_scan_plans.is_empty() {
            Err(DataFusionError::Internal(
                "No index filters applicable".to_string(),
            ))
        } else {
            // Use the created index scan plans to build the join
            log::debug!(
                "Creating index join plan with {} lookup(s)",
                index_scan_plans.len()
            );
            self.merge_indexes_streams(index_scan_plans, projection)
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // For this test provider, we *only* support filters handled by indexes.
        let indexed_columns = self.get_indexed_columns_names()?;
        let results = filters
            .iter()
            .map(|filter| {
                let cols = filter.column_refs();
                if cols.is_empty() {
                    return TableProviderFilterPushDown::Unsupported;
                }
                let all_indexed = cols.iter().all(|c| indexed_columns.contains(&c.name));
                if all_indexed {
                    // Check if the *specific* expression is supported by *any* index
                    // (this uses the default logic from IndexedTableProvider for simplicity here)
                    match self.find_suitable_indexes(filter) {
                        Ok(indexes) if !indexes.is_empty() => TableProviderFilterPushDown::Exact,
                        _ => TableProviderFilterPushDown::Unsupported,
                    }
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect();
        Ok(results)
    }
}
