use std::collections::HashSet;

use arrow::array::{Array, RecordBatch, StringArray};
use common::employee_provider::EmployeeTableProvider;
use common::setup_test_env;
use datafusion::error::Result;

mod common;

// +----+-------+--------+----------+
// | id | name  | age    | department|
// +----+-------+--------+----------+
// | 1  | Alice | 25    | Engineering|
// | 2  | Bob   | 30    | Sales      |
// | 3  |Charlie| 35    | Marketing  |
// | 4  | David | 28    | Engineering|
// | 5  | Eve   | 32    | Sales      |
// +----+-------+--------+----------+

#[tokio::test]
async fn test_employee_table_filter_age_less_than() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age < 30")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Alice", "David"]);
}

#[tokio::test]
async fn test_employee_table_filter_age_greater_equal() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age >= 30")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Bob", "Charlie", "Eve"]);
}

#[tokio::test]
async fn test_employee_table_filter_age_exact() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age = 35")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Charlie"]);
}

#[tokio::test]
async fn test_employee_table_filter_age_between() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age BETWEEN 25 AND 30")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Alice", "Bob", "David"]);
}

#[tokio::test]
async fn test_employee_table_filter_department() -> Result<()> {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT * FROM employees WHERE department = 'Engineering'")
        .await?;

    let results = df.collect().await?;

    assert_names(&results, &["Alice", "David"]);

    Ok(())
}

#[tokio::test]
async fn test_employee_table_filter_age_or() -> Result<()> {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT * FROM employees WHERE age = 25 OR age = 30")
        .await?;
    let results = df.collect().await?;

    assert_names(&results, &["Alice", "Bob"]);

    Ok(())
}

// TODO:
// .sql("SELECT * FROM employees WHERE age = 5 OR age = 10 OR age = 25 OR age = 30")

// #[tokio::test]
// async fn test_employee_table_filter_multiple_age() -> Result<()> {
//  let ctx = setup_test_env().await;

//  let df = ctx
//      .sql("SELECT * FROM employees WHERE age BETWEEN 25 AND 30 OR age = 32")
//      .await?;

//  let results = df.collect().await?;
//  assert_names(&results, &["Alice", "Bob", "David", "Eve"]);
//  Ok(())
// }

fn assert_names(results: &[RecordBatch], expected_names: &[&str]) {
    let mut names = HashSet::new();
    for batch in results {
        let name_batch = batch
            .column(1) // name is first in projection
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for name in name_batch.iter().flatten() {
            names.insert(name.to_string());
        }
    }

    for expected_name in expected_names {
        assert!(
            names.contains(*expected_name),
            "Name {} not found in {:?}",
            expected_name,
            names
        );
    }
    assert_eq!(
        names.len(),
        expected_names.len(),
        "Expected {} names: {:?}, got {} names: {:?}",
        expected_names.len(),
        expected_names,
        names.len(),
        names
    );
}
