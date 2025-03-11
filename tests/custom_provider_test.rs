use arrow::array::{Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

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
        let dept_array = StringArray::from(vec![
            "Engineering",
            "Sales",
            "Engineering",
            "Marketing",
            "Sales",
        ]);
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

    fn apply_age_filter(
        &self,
        batch: &RecordBatch,
        op: &Operator,
        value: i32,
    ) -> Result<RecordBatch> {
        let filtered_indices = self.age_index.filter_rows(op, value);

        // Create new arrays with only the filtered rows
        let new_columns: Result<Vec<Arc<dyn Array>>> = batch
            .columns()
            .iter()
            .map(|col| {
                Ok(Arc::new(arrow::compute::take(
                    col.as_ref(),
                    &Int32Array::from_iter_values(filtered_indices.iter().map(|&i| i as i32)),
                    None,
                )?) as Arc<dyn Array>)
            })
            .collect();

        Ok(RecordBatch::try_new(self.schema.clone(), new_columns?)?)
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
        let mut filtered_batch = self.batches[0].clone();

        for filter in filters {
            if let Expr::BinaryExpr(expr) = filter {
                if let (Expr::Column(col), Expr::Literal(value)) = (&*expr.left, &*expr.right) {
                    if col.name == "age" {
                        if let ScalarValue::Int32(Some(age_value)) = value {
                            filtered_batch =
                                self.apply_age_filter(&filtered_batch, &expr.op, *age_value)?;
                        }
                    }
                }
            }
        }

        // Create a memory execution plan with the filtered data
        Ok(Arc::new(MemoryExec::try_new(
            &[vec![filtered_batch]],
            self.schema(),
            projection.cloned(),
        )?))
    }

    fn supports_filter_pushdown(&self, filter: &Expr) -> Result<TableProviderFilterPushDown> {
        // Check if the filter is on the age column
        match filter {
            Expr::BinaryExpr(expr) => {
                if let (Expr::Column(col), Expr::Literal(_)) = (&*expr.left, &*expr.right) {
                    if col.name == "age" {
                        match expr.op {
                            Operator::Eq
                            | Operator::Gt
                            | Operator::GtEq
                            | Operator::Lt
                            | Operator::LtEq => {
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

#[tokio::test]
async fn test_employee_table_provider() {
    // Create a session context
    let ctx = SessionContext::new();

    // Create and register our employee table
    let provider = EmployeeTableProvider::new();
    ctx.register_table("employees", Arc::new(provider)).unwrap();

    // Test 1: Simple SELECT
    let df = ctx.sql("SELECT * FROM employees").await.unwrap();
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
