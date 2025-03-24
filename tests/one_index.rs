use arrow::array::{Array, StringArray};
use common::employee_provider::EmployeeTableProvider;
use common::setup_test_env;
use datafusion::error::Result;

mod common;

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
        .column(1) // name is first in projection
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(names.iter().any(|n| n == Some("Alice"))); // Alice is 25
    assert!(names.iter().any(|n| n == Some("David"))); // David is 28
    assert!(names.iter().any(|n| n == Some("Bob"))); // Bob is 30
    assert_eq!(names.len(), 3);
}

#[tokio::test]
async fn test_employee_table_filter_department() -> Result<()> {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT * FROM employees WHERE department = 'Engineering'")
        .await?;

    let results = df.collect().await?;
    let batch = &results[0];

    assert_eq!(batch.num_rows(), 2);

    let names: Vec<_> = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .collect();

    // Should contain Alice and David who are in Engineering
    assert!(names.contains(&Some("Alice")));
    assert!(names.contains(&Some("David")));

    Ok(())
}
