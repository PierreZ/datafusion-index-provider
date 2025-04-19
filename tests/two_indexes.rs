use arrow::array::{Array, Int32Array, StringArray};
use common::employee_provider::EmployeeTableProvider;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

mod common;

#[tokio::test]
async fn test_employee_table_filter_age_and_department() -> Result<()> {
    let ctx = SessionContext::new();
    let provider = EmployeeTableProvider::new();
    ctx.register_table("employees", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT name, age, department FROM employees WHERE age = 25 AND department = 'Engineering'")
        .await?;
    let results = df.collect().await?;

    let batch = &results[0];
    let names = batch
        .column_by_name("name")
        .expect("should find column name")
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let departments = batch
        .column_by_name("department")
        .expect("should find column department")
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ages = batch
        .column_by_name("age")
        .expect("should find column age")
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    // Check that all results match our criteria
    for i in 0..batch.num_rows() {
        assert_eq!(ages.value(i), 25, "Age should be 25");
        assert_eq!(
            departments.value(i),
            "Engineering",
            "Department should be Engineering"
        );
    }

    // We expect only Alice who is 25 and in Engineering
    assert_eq!(batch.num_rows(), 1, "Should find 1 employee");
    assert!(names.iter().any(|n| n == Some("Alice")));

    Ok(())
}

#[tokio::test]
async fn test_employee_table_filter_age_greater_than_20_and_department() -> Result<()> {
    let ctx = SessionContext::new();
    let provider = EmployeeTableProvider::new();
    ctx.register_table("employees", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT name, age, department FROM employees WHERE age > 20 AND department = 'Engineering'")
        .await?;
    let results = df.collect().await?;

    let batch = &results[0];
    let names = batch
        .column_by_name("name")
        .expect("should find column name")
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let departments = batch
        .column_by_name("department")
        .expect("should find column department")
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ages = batch
        .column_by_name("age")
        .expect("should find column age")
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    // Check that all results match our criteria
    for i in 0..batch.num_rows() {
        assert!(ages.value(i) > 20, "Age should be > 20");
        assert_eq!(
            departments.value(i),
            "Engineering",
            "Department should be Engineering"
        );
    }

    // We expect Alice (age 25) and David (age 28) who are both in Engineering
    assert_eq!(batch.num_rows(), 2, "Should find 2 employees");
    assert!(names.iter().any(|n| n == Some("Alice")));
    assert!(names.iter().any(|n| n == Some("David")));

    Ok(())
}

#[tokio::test]
async fn test_employee_table_filter_no_matches() -> Result<()> {
    let ctx = SessionContext::new();
    let provider = EmployeeTableProvider::new();
    ctx.register_table("employees", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT name, age, department FROM employees WHERE age > 40 AND department = 'Engineering'")
        .await?;
    let results = df.collect().await?;

    // We expect no results since no one is over 40 in Engineering
    assert_eq!(results.len(), 0, "Should find 0 employees");

    Ok(())
}
