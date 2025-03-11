use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;
use arrow::array::{Int32Array, StringArray, Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, Operator};
use datafusion::physical_plan::{ExecutionPlan, Statistics, SendableRecordBatchStream, DisplayAs, DisplayFormatType};
use datafusion::physical_plan::memory::{MemoryExec, MemoryStream};
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use std::fmt::{Debug, Formatter};
use async_trait::async_trait;
use futures::StreamExt;

/// Custom execution plan for index lookups
#[derive(Debug)]
struct IndexLookupExec {
    schema: SchemaRef,
    filtered_indices: Vec<usize>,
}

impl IndexLookupExec {
    fn new(schema: SchemaRef, filtered_indices: Vec<usize>) -> Self {
        IndexLookupExec {
            schema,
            filtered_indices,
        }
    }
}

impl DisplayAs for IndexLookupExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "IndexLookupExec")
    }
}

#[async_trait]
impl ExecutionPlan for IndexLookupExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute<'a>(
        &'a self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Convert indices to RecordBatch
        let indices = UInt64Array::from_iter_values(
            self.filtered_indices.iter().map(|&i| i as u64)
        );
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(indices)],
        )?;
        
        Ok(Box::pin(MemoryStream::try_new(
            vec![batch],
            self.schema.clone(),
            None,
        )?))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

/// A simple index structure that maps age values to row indices
#[derive(Debug)]
struct AgeIndex {
    // Maps age to a vector of row indices
    index: BTreeMap<i32, Vec<usize>>,
}

impl AgeIndex {
    fn new(ages: &Int32Array) -> Self {
        let mut index = BTreeMap::new();
        for (i, age) in ages.iter().enumerate() {
            if let Some(age) = age {
                index.entry(age).or_insert_with(Vec::new).push(i);
            }
        }
        AgeIndex { index }
    }

    fn filter_rows(&self, op: &Operator, value: i32) -> Vec<usize> {
        match op {
            Operator::Gt => self.index.range((value+1)..).flat_map(|(_, rows)| rows).copied().collect(),
            Operator::GtEq => self.index.range(value..).flat_map(|(_, rows)| rows).copied().collect(),
            Operator::Lt => self.index.range(..value).flat_map(|(_, rows)| rows).copied().collect(),
            Operator::LtEq => self.index.range(..=value).flat_map(|(_, rows)| rows).copied().collect(),
            Operator::Eq => self.index.get(&value).map_or(vec![], |rows| rows.clone()),
            _ => vec![], // Unsupported operators return empty vec
        }
    }
}

/// A simple in-memory table provider that stores employee data with an age index
pub struct EmployeeTableProvider {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    age_index: AgeIndex,
}

impl EmployeeTableProvider {
    pub fn new() -> Self {
        // Define schema: id, name, department, age
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("department", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]));

        // Sample employee data
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
        let dept_array = StringArray::from(vec!["Engineering", "Sales", "Engineering", "Marketing", "Sales"]);
        let age_array = Int32Array::from(vec![25, 30, 35, 28, 32]);

        // Create the age index
        let age_index = AgeIndex::new(&age_array);

        // Create record batch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(dept_array),
                Arc::new(age_array),
            ],
        )
        .unwrap();

        EmployeeTableProvider {
            schema,
            batches: vec![batch],
            age_index,
        }
    }
}

#[async_trait::async_trait]
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
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Look for age-related filters
        let mut filtered_indices = None;
        
        for filter in filters {
            if let Expr::BinaryExpr(expr) = filter {
                if let (Expr::Column(col), Expr::Literal(value)) = (&*expr.left, &*expr.right) {
                    if col.name == "age" {
                        if let ScalarValue::Int32(Some(age_value)) = value {
                            filtered_indices = Some(self.age_index.filter_rows(&expr.op, *age_value));
                        }
                    }
                }
            }
        }

        // Create the index lookup execution plan
        let index_schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::UInt64, false),
        ]));

        let index_exec = Arc::new(IndexLookupExec::new(
            index_schema,
            filtered_indices.unwrap_or_else(|| (0..self.batches[0].num_rows()).collect()),
        ));

        // Create the data fetch execution plan
        let data_exec = Arc::new(MemoryExec::try_new(
            &[self.batches.clone()],
            self.schema(),
            projection.cloned(),
        )?);

        // Return both execution plans
        Ok(Arc::new(IndexJoinExec::new(index_exec, data_exec)))
    }

    fn supports_filter_pushdown(
        &self,
        filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        // Check if the filter is on the age column
        match filter {
            Expr::BinaryExpr(expr) => {
                if let (Expr::Column(col), Expr::Literal(_)) = (&*expr.left, &*expr.right) {
                    if col.name == "age" {
                        match expr.op {
                            Operator::Eq | Operator::Gt | Operator::GtEq |
                            Operator::Lt | Operator::LtEq => {
                                return Ok(TableProviderFilterPushDown::Exact);
                            }
                            _ => {}
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}

/// Execution plan that joins index results with actual data
struct IndexJoinExec {
    index_exec: Arc<dyn ExecutionPlan>,
    data_exec: Arc<dyn ExecutionPlan>,
}

impl IndexJoinExec {
    fn new(index_exec: Arc<dyn ExecutionPlan>, data_exec: Arc<dyn ExecutionPlan>) -> Self {
        IndexJoinExec {
            index_exec,
            data_exec,
        }
    }
}

impl DisplayAs for IndexJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "IndexJoinExec")
    }
}

impl Debug for IndexJoinExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IndexJoinExec")
    }
}

#[async_trait]
impl ExecutionPlan for IndexJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.data_exec.schema()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.index_exec.clone(), self.data_exec.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IndexJoinExec::new(
            children[0].clone(),
            children[1].clone(),
        )))
    }

    fn execute<'a>(
        &'a self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Execute index lookup
        let mut index_stream = self.index_exec.execute(partition, context.clone())?;
        let mut batches = Vec::new();
        
        // Since execute is not async, we'll collect synchronously
        while let Some(batch) = futures::executor::block_on(index_stream.next()) {
            batches.push(batch?);
        }
        
        let row_ids: Vec<usize> = batches
            .iter()
            .flat_map(|batch: &RecordBatch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .iter()
                    .map(|id| id.unwrap() as usize)
            })
            .collect();

        // Execute data fetch with filtered row IDs
        let mut data_stream = self.data_exec.execute(partition, context)?;
        let mut data_batches = Vec::new();
        
        // Collect data batches synchronously
        while let Some(batch) = futures::executor::block_on(data_stream.next()) {
            data_batches.push(batch?);
        }
        
        // Filter data using row IDs
        let filtered_batch = apply_row_filter(&data_batches[0], &row_ids)?;
        
        Ok(Box::pin(MemoryStream::try_new(
            vec![filtered_batch],
            self.schema(),
            None,
        )?))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

fn apply_row_filter(batch: &RecordBatch, row_ids: &[usize]) -> Result<RecordBatch> {
    let new_columns: Result<Vec<Arc<dyn Array>>> = batch
        .columns()
        .iter()
        .map(|col| {
            Ok(Arc::new(arrow::compute::take(
                col.as_ref(),
                &Int32Array::from_iter_values(row_ids.iter().map(|&i| i as i32)),
                None,
            )?) as Arc<dyn Array>)
        })
        .collect();

    Ok(RecordBatch::try_new(batch.schema(), new_columns?)?)
}

#[tokio::test]
async fn test_employee_table_provider() {
    // Create a session context
    let ctx = SessionContext::new();

    // Create and register our employee table
    let provider = EmployeeTableProvider::new();
    ctx.register_table("employees", Arc::new(provider))
        .unwrap();

    // Test 1: Simple SELECT
    let df = ctx
        .sql("SELECT * FROM employees")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(results[0].num_rows(), 5);
    assert_eq!(results[0].num_columns(), 4);

    // Test 2: Filter by age using index (less than)
    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age < 30")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    let names = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(names.iter().any(|n| n == Some("Alice"))); // Alice is 25
    assert!(names.iter().any(|n| n == Some("David"))); // David is 28

    // Test 3: Filter by age using index (greater than or equal)
    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age >= 30")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    let names = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(names.iter().any(|n| n == Some("Bob"))); // Bob is 30
    assert!(names.iter().any(|n| n == Some("Charlie"))); // Charlie is 35
    assert!(names.iter().any(|n| n == Some("Eve"))); // Eve is 32

    // Test 4: Exact age match using index
    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age = 35")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    let names = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(names.iter().any(|n| n == Some("Charlie"))); // Only Charlie is 35
    assert_eq!(names.len() as usize, 1);
}
