mod common;
use crate::common::assert_names;
use common::{assert_ages, setup_test_env};

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
async fn test_employee_table_filter_age_equal() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age = 25")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Alice"]);
    assert_ages(&results, &[25]);
}

#[tokio::test]
async fn test_employee_table_filter_age_greater_than() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age > 30")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Charlie", "Eve"]);
    assert_ages(&results, &[35, 32]);
}

#[tokio::test]
async fn test_employee_table_filter_age_less_than() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age < 30")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Alice", "David"]);
    assert_ages(&results, &[25, 28]);
}

#[tokio::test]
async fn test_employee_table_filter_age_greater_than_or_equal() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age >= 30")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Bob", "Charlie", "Eve"]);
    assert_ages(&results, &[30, 35, 32]);
}

#[tokio::test]
async fn test_employee_table_filter_age_less_than_or_equal() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age <= 30")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Alice", "Bob", "David"]);
    assert_ages(&results, &[25, 30, 28]);
}

#[tokio::test]
async fn test_employee_table_filter_age_between() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age BETWEEN 28 AND 32")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    // Employees: David (28), Bob (30), Eve (32)
    assert_names(&results, &["David", "Bob", "Eve"]);
    assert_ages(&results, &[28, 30, 32]);
}

#[tokio::test]
async fn test_employee_table_filter_department_equal() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE department = 'Engineering' ORDER BY name")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Alice", "David"]);
    assert_ages(&results, &[25, 28]);
}

#[tokio::test]
async fn test_employee_table_filter_department_equal_and_young() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE department = 'Engineering' AND age < 26 ORDER BY name")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Alice"]);
    assert_ages(&results, &[25]);
}

#[tokio::test]
async fn test_employee_table_filter_department_equal_and_all_ages() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE department = 'Engineering' AND age > 10 ORDER BY name")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Alice", "David"]);
    assert_ages(&results, &[25, 28]);
}
