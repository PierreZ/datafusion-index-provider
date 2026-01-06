mod common;
use crate::common::assert_names;
use common::{
    assert_ages, assert_departments, extract_names, setup_test_env, setup_test_env_sequential,
};

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
        "Expected 2 rows after deduplication, but found {total_rows}"
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
    assert_eq!(total_rows, 2, "Expected exactly 2 rows, got {total_rows}");

    // Verify we get the expected employees
    assert_names(&results, &["Alice", "David"]);
    assert_ages(&results, &[25, 28]);

    println!("Complex AND/OR query with precise filtering results: {results:?}");
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
    assert!(total_rows >= 1, "Expected at least 1 row, got {total_rows}");

    // Should include Alice (25, Engineering) and David (28, Engineering)
    assert_names(&results, &["Alice", "David"]);
    assert_ages(&results, &[25, 28]);
}

#[tokio::test]
async fn test_employee_table_filter_complex_and_or_mixed() {
    let ctx = setup_test_env().await;

    // Complex query that previously caused UnionExec schema mismatch
    // (age > 30 AND department = 'Engineering') creates HashJoinExec with 2 __row_id__ columns
    // department = 'Sales' creates IndexScanExec with 1 __row_id__ column
    // Our fix adds ProjectionExec to normalize schemas before UnionExec
    let df = ctx
        .sql(
            "SELECT name, age, department FROM employees WHERE 
            (age > 30 AND department = 'Engineering') OR department = 'Sales'",
        )
        .await
        .unwrap();

    let results = df.collect().await.unwrap();

    // Should return:
    // - No one from the AND condition (no Engineering employees > 30)
    // - Bob (30, Sales) and Eve (32, Sales) from the OR condition
    let total_rows = results.iter().map(|b| b.num_rows()).sum::<usize>();
    assert_eq!(total_rows, 2, "Expected 2 rows, got {total_rows}");

    assert_names(&results, &["Bob", "Eve"]);
    assert_ages(&results, &[30, 32]);
    assert_departments(&results, &["Sales"]);
}

#[tokio::test]
async fn test_employee_table_filter_multiple_and_conditions_in_or() {
    let ctx = setup_test_env().await;

    // Multiple AND conditions within OR - all creating HashJoinExec plans
    let df = ctx
        .sql(
            "SELECT name, age, department FROM employees WHERE 
            (age >= 25 AND department = 'Engineering') OR 
            (age >= 30 AND department = 'Sales')",
        )
        .await
        .unwrap();

    let results = df.collect().await.unwrap();

    // Should return:
    // - Alice (25, Engineering) and David (28, Engineering) from first AND
    // - Bob (30, Sales) and Eve (32, Sales) from second AND
    let total_rows = results.iter().map(|b| b.num_rows()).sum::<usize>();
    assert_eq!(total_rows, 4, "Expected 4 rows, got {total_rows}");

    assert_names(&results, &["Alice", "Bob", "David", "Eve"]);
    assert_ages(&results, &[25, 30, 28, 32]);
    assert_departments(&results, &["Engineering", "Sales"]);
}

#[tokio::test]
async fn test_employee_table_filter_and_or_with_simple_conditions() {
    let ctx = setup_test_env().await;

    // Mix of AND condition and simple OR conditions
    let df = ctx
        .sql(
            "SELECT name, age FROM employees WHERE 
            (age < 28 AND department = 'Engineering') OR 
            department = 'Sales' OR 
            age = 28",
        )
        .await
        .unwrap();

    let results = df.collect().await.unwrap();

    // Should return:
    // - Alice (25, Engineering) from the AND condition
    // - Bob (30, Sales) and Eve (32, Sales) from department = 'Sales'
    // - David (28, Engineering) from age = 28
    let total_rows = results.iter().map(|b| b.num_rows()).sum::<usize>();
    assert_eq!(total_rows, 4, "Expected 4 rows, got {total_rows}");

    assert_names(&results, &["Alice", "Bob", "David", "Eve"]);
    assert_ages(&results, &[25, 30, 28, 32]);
    // Note: Only projecting name, age columns in this test
}

#[tokio::test]
async fn test_employee_table_filter_nested_and_or_schema_normalization() {
    let ctx = setup_test_env().await;

    // Very complex nested query that creates multiple schema types
    let df = ctx
        .sql(
            "SELECT name, department FROM employees WHERE 
            ((age > 25 AND age < 35) AND department = 'Engineering') OR 
            (department = 'Sales' OR department = 'Marketing')",
        )
        .await
        .unwrap();

    let results = df.collect().await.unwrap();

    // Should return David (28, Engineering) and Charlie (35, Marketing), Bob (30, Sales), Eve (32, Sales)
    // From: ((age > 25 AND age < 35) AND department = 'Engineering') OR (department = 'Sales' OR department = 'Marketing')
    let total_rows = results.iter().map(|b| b.num_rows()).sum::<usize>();
    assert_eq!(total_rows, 4, "Expected 4 rows, got {total_rows}");

    assert_names(&results, &["Bob", "Charlie", "David", "Eve"]);
    assert_departments(&results, &["Engineering", "Marketing", "Sales"]);
}

#[tokio::test]
async fn test_employee_table_filter_triple_or_with_and_conditions() {
    let ctx = setup_test_env().await;

    // Three-way OR with different complexity levels
    let df = ctx
        .sql(
            "SELECT name, age, department FROM employees WHERE 
            (age = 25 AND department = 'Engineering') OR 
            (age > 30 AND department = 'Sales') OR 
            department = 'Marketing'",
        )
        .await
        .unwrap();

    let results = df.collect().await.unwrap();

    // Should return:
    // - Alice (25, Engineering) from (age = 25 AND department = 'Engineering')
    // - Eve (32, Sales) from (age > 30 AND department = 'Sales') - Bob is 30, not > 30
    // - Charlie (35, Marketing) from department = 'Marketing'
    let total_rows = results.iter().map(|b| b.num_rows()).sum::<usize>();
    assert_eq!(total_rows, 3, "Expected 3 rows, got {total_rows}");

    assert_names(&results, &["Alice", "Charlie", "Eve"]);
    assert_ages(&results, &[25, 35, 32]);
    assert_departments(&results, &["Engineering", "Marketing", "Sales"]);
}

#[tokio::test]
async fn test_employee_table_filter_all_and_conditions_in_or() {
    let ctx = setup_test_env().await;

    // All OR branches are AND conditions - all should create HashJoinExec
    let df = ctx
        .sql(
            "SELECT name FROM employees WHERE 
            (age >= 25 AND department = 'Engineering') OR 
            (age >= 30 AND department = 'Sales') OR
            (age >= 28 AND department = 'Marketing')",
        )
        .await
        .unwrap();

    let results = df.collect().await.unwrap();

    // Should return:
    // - Alice (25, Engineering) and David (28, Engineering) from (age >= 25 AND department = 'Engineering')
    // - Bob (30, Sales) and Eve (32, Sales) from (age >= 30 AND department = 'Sales')
    // - Charlie (35, Marketing) from (age >= 28 AND department = 'Marketing')
    let total_rows = results.iter().map(|b| b.num_rows()).sum::<usize>();
    assert_eq!(total_rows, 5, "Expected 5 rows, got {total_rows}");

    assert_names(&results, &["Alice", "Bob", "Charlie", "David", "Eve"]);
}

#[tokio::test]
async fn test_employee_table_filter_complex_and_or_deduplication() {
    let ctx = setup_test_env().await;

    // Query that might return overlapping results - test deduplication
    let df = ctx
        .sql(
            "SELECT name, age, department FROM employees WHERE 
            (age >= 30 AND department = 'Engineering') OR 
            (department = 'Engineering' AND age > 25)",
        )
        .await
        .unwrap();

    let results = df.collect().await.unwrap();

    // Query: (age >= 30 AND department = 'Engineering') OR (department = 'Engineering' AND age > 25)
    // Only David (28, Engineering) matches both branches
    // Alice (25, Engineering) matches the second branch but not the first (age < 30)
    // However both branches are for Engineering, so Alice matches the second branch
    // Wait, let me check the logs again...actually only David (row_id 4) was returned
    let total_rows = results.iter().map(|b| b.num_rows()).sum::<usize>();
    assert_eq!(total_rows, 1, "Expected 1 row, got {total_rows}");

    assert_names(&results, &["David"]);
    assert_ages(&results, &[28]);
    assert_departments(&results, &["Engineering"]);

    // Verify no duplicate names (proper deduplication)
    let names = extract_names(&results);
    let unique_names: std::collections::HashSet<_> = names.iter().collect();
    assert_eq!(names.len(), unique_names.len(), "Found duplicate results");
}

#[tokio::test]
async fn test_employee_table_filter_four_way_or_schema_stress_test() {
    let ctx = setup_test_env().await;

    // Four-way OR with maximum schema diversity to stress test normalization
    let df = ctx
        .sql(
            "SELECT name FROM employees WHERE 
            (age = 25 AND department = 'Engineering') OR 
            (age >= 30 AND department = 'Sales') OR
            department = 'Marketing' OR
            age = 28",
        )
        .await
        .unwrap();

    let results = df.collect().await.unwrap();

    // Should return:
    // - Alice (25, Engineering) from (age = 25 AND department = 'Engineering')
    // - Bob (30, Sales) and Eve (32, Sales) from (age >= 30 AND department = 'Sales')
    // - Charlie (35, Marketing) from department = 'Marketing'
    // - David (28, Engineering) from age = 28
    let total_rows = results.iter().map(|b| b.num_rows()).sum::<usize>();
    assert_eq!(total_rows, 5, "Expected 5 rows, got {total_rows}");

    assert_names(&results, &["Alice", "Bob", "Charlie", "David", "Eve"]);
}

#[tokio::test]
async fn test_employee_table_filter_deeply_nested_and_or_schema_normalization() {
    let ctx = setup_test_env().await;

    // Deeply nested query with multiple levels of AND/OR
    let df = ctx
        .sql(
            "SELECT name, age FROM employees WHERE 
            ((age > 25 AND age < 30) OR (age > 30 AND age < 35)) AND 
            (department = 'Engineering' OR department = 'Sales')",
        )
        .await
        .unwrap();

    let results = df.collect().await.unwrap();

    // Query: ((age > 25 AND age < 30) OR (age > 30 AND age < 35)) AND (department = 'Engineering' OR department = 'Sales')
    // This simplifies to: (age 26-29 OR age 31-34) AND (Engineering OR Sales)
    // Results: David (28, Engineering) and Eve (32, Sales)
    let total_rows = results.iter().map(|b| b.num_rows()).sum::<usize>();
    assert_eq!(total_rows, 2, "Expected 2 rows, got {total_rows}");

    assert_names(&results, &["David", "Eve"]);
    assert_ages(&results, &[28, 32]);
}

#[tokio::test]
async fn test_employee_table_filter_edge_case_single_and_in_or() {
    let ctx = setup_test_env().await;

    // Edge case: single AND condition should work without UnionExec
    let df = ctx
        .sql(
            "SELECT name, department FROM employees WHERE 
            age > 30 AND department = 'Engineering'",
        )
        .await
        .unwrap();

    let results = df.collect().await.unwrap();

    // Should return no one (Charlie is in Marketing, not Engineering, and no Engineering employees > 30)
    let total_rows = results.iter().map(|b| b.num_rows()).sum::<usize>();
    assert_eq!(total_rows, 0, "Expected 0 rows, got {total_rows}");
}

#[tokio::test]
async fn test_employee_table_filter_all_simple_or_conditions() {
    let ctx = setup_test_env().await;

    // All simple conditions - should all create IndexScanExec with same schema
    let df = ctx
        .sql(
            "SELECT name, age FROM employees WHERE 
            department = 'Engineering' OR 
            department = 'Sales' OR 
            age = 25 OR 
            age = 32",
        )
        .await
        .unwrap();

    let results = df.collect().await.unwrap();

    // Should return:
    // - Alice (25, Engineering) and David (28, Engineering) from department = 'Engineering'
    // - Bob (30, Sales) and Eve (32, Sales) from department = 'Sales'
    // - Alice (25) again from age = 25
    // - Eve (32) again from age = 32
    // But deduplication should give us: Alice, Bob, David, Eve
    let total_rows = results.iter().map(|b| b.num_rows()).sum::<usize>();
    assert_eq!(total_rows, 4, "Expected 4 rows, got {total_rows}");

    assert_names(&results, &["Alice", "Bob", "David", "Eve"]);
    assert_ages(&results, &[25, 30, 28, 32]);
}

// =============================================================================
// Sequential Union Mode Tests
// =============================================================================
// These tests verify that UnionMode::Sequential works correctly by using
// SequentialUnionExec instead of the parallel UnionExec for OR conditions.

#[tokio::test]
async fn test_sequential_union_mode_simple_or() {
    let ctx = setup_test_env_sequential().await;

    // Simple OR query that triggers SequentialUnionExec
    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age = 25 OR age = 35")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();

    assert_names(&results, &["Alice", "Charlie"]);
    assert_ages(&results, &[25, 35]);
}

#[tokio::test]
async fn test_sequential_union_mode_multiple_or() {
    let ctx = setup_test_env_sequential().await;

    // Multiple OR conditions
    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age = 25 OR age = 30 OR age = 35")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();

    assert_names(&results, &["Alice", "Bob", "Charlie"]);
    assert_ages(&results, &[25, 30, 35]);
}

#[tokio::test]
async fn test_sequential_union_mode_mixed_indexes() {
    let ctx = setup_test_env_sequential().await;

    // OR across different indexes (age and department)
    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age = 25 OR department = 'Sales'")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();

    assert_names(&results, &["Alice", "Bob", "Eve"]);
    assert_ages(&results, &[25, 30, 32]);
}

#[tokio::test]
async fn test_sequential_union_mode_complex_and_or() {
    let ctx = setup_test_env_sequential().await;

    // Complex query with AND conditions inside OR - tests schema normalization
    let df = ctx
        .sql(
            "SELECT name, age, department FROM employees WHERE
            (age >= 25 AND department = 'Engineering') OR
            (age >= 30 AND department = 'Sales')",
        )
        .await
        .unwrap();
    let results = df.collect().await.unwrap();

    // Should return:
    // - Alice (25, Engineering) and David (28, Engineering) from first AND
    // - Bob (30, Sales) and Eve (32, Sales) from second AND
    let total_rows = results.iter().map(|b| b.num_rows()).sum::<usize>();
    assert_eq!(total_rows, 4, "Expected 4 rows, got {total_rows}");

    assert_names(&results, &["Alice", "Bob", "David", "Eve"]);
    assert_ages(&results, &[25, 30, 28, 32]);
    assert_departments(&results, &["Engineering", "Sales"]);
}

#[tokio::test]
async fn test_sequential_union_mode_deduplication() {
    let ctx = setup_test_env_sequential().await;

    // Query with overlapping conditions - tests that deduplication works
    let df = ctx
        .sql("SELECT name, age FROM employees WHERE age = 25 OR age < 29")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();

    // Alice (25) matches both conditions, should appear only once
    let total_rows = results.iter().map(|b| b.num_rows()).sum::<usize>();
    assert_eq!(
        total_rows, 2,
        "Expected 2 rows after deduplication, got {total_rows}"
    );

    assert_names(&results, &["Alice", "David"]);
    assert_ages(&results, &[25, 28]);
}
