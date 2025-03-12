use arrow::array::{Array, Int32Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion_index_provider::*;
use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::physical_plan::memory::MemoryStream;

fn compute_properties(schema: SchemaRef) -> PlanProperties {
    let eq_properties = EquivalenceProperties::new(schema);
    PlanProperties::new(
        eq_properties,
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    )
}

/// Custom execution plan for index lookups
#[derive(Debug)]
struct IndexLookupExec {
    schema: SchemaRef,
    filtered_indices: Vec<usize>,
    cache: PlanProperties,
}

impl IndexLookupExec {
    fn new(schema: SchemaRef, filtered_indices: Vec<usize>) -> Self {
        IndexLookupExec {
            schema: schema.clone(),
            filtered_indices,
            cache: compute_properties(schema),
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
    fn name(&self) -> &str {
        "IndexLookupExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Convert indices to RecordBatch
        let indices =
            UInt64Array::from_iter_values(self.filtered_indices.iter().map(|&i| i as u64));
        let batch = RecordBatch::try_new(self.schema.clone(), vec![Arc::new(indices)])?;

        Ok(Box::pin(MemoryStream::try_new(
            vec![batch],
            self.schema.clone(),
            None,
        )?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
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
            Operator::Gt => self
                .index
                .range((value + 1)..)
                .flat_map(|(_, rows)| rows)
                .copied()
                .collect(),
            Operator::GtEq => self
                .index
                .range(value..)
                .flat_map(|(_, rows)| rows)
                .copied()
                .collect(),
            Operator::Lt => self
                .index
                .range(..value)
                .flat_map(|(_, rows)| rows)
                .copied()
                .collect(),
            Operator::LtEq => self
                .index
                .range(..=value)
                .flat_map(|(_, rows)| rows)
                .copied()
                .collect(),
            Operator::Eq => self.index.get(&value).map_or(vec![], |rows| rows.clone()),
            _ => unimplemented!(),
        }
    }
}

/// A simple in-memory table provider that stores employee data with an age index
#[derive(Debug)]
pub struct EmployeeTableProvider {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    age_index: AgeIndex,
}

#[async_trait]
impl IndexProvider for EmployeeTableProvider {
    fn get_indexed_columns(&self) -> HashMap<String, IndexedColumn> {
        let mut map = HashMap::new();
        map.insert(
            "age".to_string(),
            IndexedColumn {
                name: "age".to_string(),
                supported_operators: vec![
                    Operator::Eq,
                    Operator::Gt,
                    Operator::GtEq,
                    Operator::Lt,
                    Operator::LtEq,
                ],
            },
        );
        map
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

        // Create record batch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(age_array.clone()),
                Arc::new(department_array),
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
        log::debug!("Checking filters: {:?}", &filters);
        // Look for age-related filters
        let mut filtered_indices = None;

        for filter in filters {
            if let Expr::BinaryExpr(expr) = filter {
                if let (Expr::Column(col), Expr::Literal(value)) = (&*expr.left, &*expr.right) {
                    if self.supports_index_operator(&col.name, &expr.op) {
                        if let ScalarValue::Int32(Some(age_value)) = value {
                            filtered_indices =
                                Some(self.age_index.filter_rows(&expr.op, *age_value));
                        }
                    }
                }
            }
        }

        log::debug!("Filtered indices: {:?}", &filtered_indices);

        // Create the index lookup execution plan
        let index_schema = Arc::new(Schema::new(vec![Field::new(
            "row_id",
            DataType::UInt64,
            false,
        )]));

        let index_exec = Arc::new(IndexLookupExec::new(
            index_schema,
            filtered_indices.unwrap_or_else(|| (0..self.batches[0].num_rows()).collect()),
        ));

        // Return the execution plan
        Ok(Arc::new(IndexJoinExec::new(
            index_exec,
            self.batches.clone(),
            projection.cloned(),
            self.schema(),
        )))
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
                    if self.supports_index_operator(&col.name, &expr.op) {
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

/// Mapper that filters batches using index results
struct BatchMapper {
    batches: Vec<RecordBatch>,
}

impl BatchMapper {
    fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches }
    }
}

#[async_trait]
impl MapIndexWithRecord for BatchMapper {
    async fn map_index_with_record(&mut self, index_batch: RecordBatch) -> Result<RecordBatch> {
        // Get row indices from the index batch
        let indices = index_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let row_ids: Vec<usize> = indices.iter().flatten().map(|i| i as usize).collect();

        // Apply the row filter to get filtered batch
        apply_row_filter(&self.batches[0], &row_ids)
    }
}

/// Execution plan that joins index results with actual data
#[derive(Debug)]
struct IndexJoinExec {
    index_exec: Arc<dyn ExecutionPlan>,
    batches: Vec<RecordBatch>,
    projection: Option<Vec<usize>>,
    schema: SchemaRef,
    cache: PlanProperties,
}

impl IndexJoinExec {
    fn new(
        index_exec: Arc<dyn ExecutionPlan>,
        batches: Vec<RecordBatch>,
        projection: Option<Vec<usize>>,
        schema: SchemaRef,
    ) -> Self {
        IndexJoinExec {
            index_exec,
            batches,
            projection,
            schema: schema.clone(),
            cache: compute_properties(schema),
        }
    }
}

impl DisplayAs for IndexJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "IndexJoinExec")
    }
}

#[async_trait]
impl ExecutionPlan for IndexJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.index_exec]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IndexJoinExec::new(
            children[0].clone(),
            self.batches.clone(),
            self.projection.clone(),
            self.schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Execute index lookup to get the row indices
        let index_stream = self.index_exec.execute(partition, context.clone())?;

        // Create a mapper that will use the index results to filter batches
        let mapper = Box::new(BatchMapper::new(self.batches.clone()));

        // Create and return a ScanWithIndexStream that combines the index results with the actual data
        Ok(Box::pin(ScanWithIndexStream::new(
            index_stream,
            partition,
            mapper,
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn name(&self) -> &str {
        "IndexJoinExec"
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
}

fn apply_row_filter(batch: &RecordBatch, row_ids: &[usize]) -> Result<RecordBatch> {
    log::debug!("Row ids: {:?}", row_ids);
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

/// Helper function to setup test environment
async fn setup_test_env() -> SessionContext {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .try_init();

    // Create a session context
    let ctx = SessionContext::new();

    // Create and register our employee table
    let provider = EmployeeTableProvider::new();
    ctx.register_table("employees", Arc::new(provider)).unwrap();

    ctx
}

#[tokio::test]
async fn test_employee_table_basic_select() {
    let ctx = setup_test_env().await;

    let df = ctx.sql("SELECT * FROM employees").await.unwrap();
    let results = df.collect().await.unwrap();
    assert_eq!(results[0].num_rows(), 5);
    assert_eq!(results[0].num_columns(), 4);
}

#[tokio::test]
async fn test_employee_table_filter_age_less_than() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age < 30")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    let names = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(names.iter().any(|n| n == Some("Alice"))); // Alice is 25
    assert!(names.iter().any(|n| n == Some("David"))); // David is 28
}

#[tokio::test]
async fn test_employee_table_filter_age_greater_equal() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age >= 30")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    let names = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(names.iter().any(|n| n == Some("Bob"))); // Bob is 30
    assert!(names.iter().any(|n| n == Some("Charlie"))); // Charlie is 35
    assert!(names.iter().any(|n| n == Some("Eve"))); // Eve is 32
}

#[tokio::test]
async fn test_employee_table_filter_age_exact() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age = 35")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    let names = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(names.iter().any(|n| n == Some("Charlie"))); // Only Charlie is 35
    assert_eq!(names.len() as usize, 1);
}

#[tokio::test]
async fn test_employee_table_filter_age_between() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age BETWEEN 25 AND 30")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    let names = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(names.iter().any(|n| n == Some("Alice"))); // Alice is 25
    assert!(names.iter().any(|n| n == Some("David"))); // David is 28
    assert!(names.iter().any(|n| n == Some("Bob"))); // Bob is 30
    assert_eq!(names.len() as usize, 3);
}
