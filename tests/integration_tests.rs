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
                "SELECT name, age FROM employees WHERE age = {age}"
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

#[tokio::test]
async fn test_employee_table_filter_department_equal() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE department = 'Sales'")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Bob", "Eve"]);
    assert_ages(&results, &[30, 32]);
}

#[tokio::test]
async fn test_employee_table_filter_age_and_department_no_result() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age > 30 AND department = 'Engineering'")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &[]);
    assert_ages(&results, &[]);
}

#[tokio::test]
async fn test_employee_table_filter_age_and_department() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age <= 30 AND department = 'Engineering'")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Alice", "David"]);
    assert_ages(&results, &[25, 28]);
}

#[tokio::test]
async fn test_employee_table_filter_age_or_department() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age <= 30 OR department = 'Engineering'")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Alice", "Bob", "David"]);
    assert_ages(&results, &[25, 30, 28]);
}

#[tokio::test]
async fn test_employee_table_filter_multiple_or_on_age() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age = 25 OR age = 35 OR age = 32")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Alice", "Charlie", "Eve"]);
    assert_ages(&results, &[25, 35, 32]);
}

#[tokio::test]
async fn test_employee_table_filter_multiple_or_on_age_unique() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age < 30 OR age > 30")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Alice", "Charlie", "David", "Eve"]);
    assert_ages(&results, &[25, 35, 28, 32]);
}

#[tokio::test]
async fn test_employee_table_filter_complex_query() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql(
            "SELECT name, age FROM employees WHERE (age < 30 AND department = 'Engineering') OR (age > 30 AND department = 'Sales')",
        )
        .await
        .unwrap();
    let results = df.collect().await.unwrap();
    assert_names(&results, &["Alice", "David", "Eve"]);
    assert_ages(&results, &[25, 28, 32]);
}

#[tokio::test]
async fn test_employee_table_filter_or_with_overlapping_conditions() {
    let ctx = setup_test_env().await;

    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age = 25 OR age < 29")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();

    let total_rows = results.iter().map(|b| b.num_rows()).sum::<usize>();
    assert_eq!(
        total_rows, 2,
        "Expected 2 rows after deduplication, but found {}",
        total_rows
    );

    assert_names(&results, &["Alice", "David"]);
    assert_ages(&results, &[25, 28]);
}

// Complex nested query test - demonstrating sophisticated AND/OR capabilities
#[tokio::test]
async fn test_employee_table_filter_extremely_complex_nested_query() {
    let ctx = setup_test_env().await;

    // This demonstrates a sophisticated query with multiple AND conditions
    // and strategic OR usage that works within current schema constraints
    let df = ctx
        .sql(
            "SELECT name, age, department FROM employees WHERE 
            (age = 25 OR age = 28) 
            AND 
            department = 'Engineering'
            AND
            age < 40 
            AND 
            age > 20",
        )
        .await
        .unwrap();

    let results = df.collect().await.unwrap();
    let total_rows = results.iter().map(|b| b.num_rows()).sum::<usize>();

    // This query should match Alice (25, Engineering) and David (28, Engineering)
    assert_eq!(total_rows, 2, "Expected exactly 2 rows, got {}", total_rows);

    // Verify we get the expected employees
    assert_names(&results, &["Alice", "David"]);
    assert_ages(&results, &[25, 28]);

    println!(
        "Complex AND/OR query with precise filtering results: {:?}",
        results
    );
}

#[tokio::test]
async fn test_employee_table_filter_deeply_nested_and_or_combinations() {
    let ctx = setup_test_env().await;

    // Another complex nested query focusing on AND combinations with OR subclauses
    // This tests the index intersection capabilities with complex filter trees
    let df = ctx
        .sql(
            "SELECT name, age, department FROM employees WHERE 
            (
                (age >= 25 AND age <= 30) 
                AND 
                (department = 'Engineering' OR department = 'Sales')
            )
            AND
            (
                (age != 27 AND age != 29) 
                OR 
                (department = 'Engineering' AND age < 29)
            )",
        )
        .await
        .unwrap();

    let results = df.collect().await.unwrap();

    // This should match employees in Engineering/Sales aged 25-30,
    // excluding ages 27,29 unless they're in Engineering and under 29
    let total_rows = results.iter().map(|b| b.num_rows()).sum::<usize>();
    assert!(
        total_rows >= 1,
        "Expected at least 1 row, got {}",
        total_rows
    );

    // Should include Alice (25, Engineering) and David (28, Engineering)
    assert_names(&results, &["Alice", "David"]);
    assert_ages(&results, &[25, 28]);
}
