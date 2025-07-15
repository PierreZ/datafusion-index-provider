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
    // create a loop with tuples containing age and name to test equality
    let test_cases = vec![
        (25, "Alice"),
        (30, "Bob"),
        (35, "Charlie"),
        (28, "David"),
        (32, "Eve"),
    ];

    for (age, name) in test_cases {
        let ctx = setup_test_env().await;

        let df = ctx
            .sql(&format!(
                "SELECT name, age FROM employees WHERE age = {}",
                age
            ))
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_names(&results, &[name]);
        assert_ages(&results, &[age]);
    }
}

#[tokio::test]
async fn test_employee_table_filter_age_gt() {
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
async fn test_employee_table_filter_age_lt() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age < 30")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Alice", "David"]);
    assert_ages(&results, &[25, 28]);
}
