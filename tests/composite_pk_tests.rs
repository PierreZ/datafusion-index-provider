//! Integration tests for composite primary key support.
//!
//! These tests validate that the index provider correctly handles primary keys
//! composed of multiple columns (e.g., `(tenant_id, employee_id)`).

mod common;

use common::setup_composite_pk_test_env;
use datafusion_common::assert_batches_sorted_eq;

// Multi-tenant employee table with composite PK (tenant_id, employee_id):
//
// | tenant_id | employee_id | name    | age | department  |
// |-----------|-------------|---------|-----|-------------|
// | acme      | 1           | Alice   | 25  | Engineering |
// | acme      | 2           | Bob     | 30  | Sales       |
// | acme      | 3           | Charlie | 35  | Marketing   |
// | globex    | 1           | David   | 28  | Engineering |
// | globex    | 2           | Eve     | 32  | Sales       |

#[tokio::test]
async fn test_composite_pk_single_index_scan() {
    let ctx = setup_composite_pk_test_env().await;

    let results = ctx
        .sql("SELECT * FROM employees WHERE age = 30")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+-----------+-------------+------+-----+------------+",
            "| tenant_id | employee_id | name | age | department |",
            "+-----------+-------------+------+-----+------------+",
            "| acme      | 2           | Bob  | 30  | Sales      |",
            "+-----------+-------------+------+-----+------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_composite_pk_single_index_range() {
    let ctx = setup_composite_pk_test_env().await;

    let results = ctx
        .sql("SELECT * FROM employees WHERE age > 30")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+-----------+-------------+---------+-----+------------+",
            "| tenant_id | employee_id | name    | age | department |",
            "+-----------+-------------+---------+-----+------------+",
            "| acme      | 3           | Charlie | 35  | Marketing  |",
            "| globex    | 2           | Eve     | 32  | Sales      |",
            "+-----------+-------------+---------+-----+------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_composite_pk_and_intersection() {
    // age <= 30 AND department = 'Engineering'
    // age <= 30: Alice(acme,1), Bob(acme,2), David(globex,1)
    // Engineering: Alice(acme,1), David(globex,1)
    // Intersection: Alice, David
    let ctx = setup_composite_pk_test_env().await;

    let results = ctx
        .sql("SELECT * FROM employees WHERE age <= 30 AND department = 'Engineering'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+-----------+-------------+-------+-----+-------------+",
            "| tenant_id | employee_id | name  | age | department  |",
            "+-----------+-------------+-------+-----+-------------+",
            "| acme      | 1           | Alice | 25  | Engineering |",
            "| globex    | 1           | David | 28  | Engineering |",
            "+-----------+-------------+-------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_composite_pk_or_deduplication() {
    // age <= 25 OR department = 'Engineering'
    // age <= 25: Alice(acme,1)
    // Engineering: Alice(acme,1), David(globex,1)
    // Union (deduped): Alice, David
    let ctx = setup_composite_pk_test_env().await;

    let results = ctx
        .sql("SELECT * FROM employees WHERE age <= 25 OR department = 'Engineering'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+-----------+-------------+-------+-----+-------------+",
            "| tenant_id | employee_id | name  | age | department  |",
            "+-----------+-------------+-------+-----+-------------+",
            "| acme      | 1           | Alice | 25  | Engineering |",
            "| globex    | 1           | David | 28  | Engineering |",
            "+-----------+-------------+-------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_composite_pk_or_no_overlap() {
    // department = 'Marketing' OR department = 'Sales'
    // Marketing: Charlie(acme,3)
    // Sales: Bob(acme,2), Eve(globex,2)
    // No overlap, result: Bob, Charlie, Eve
    let ctx = setup_composite_pk_test_env().await;

    let results = ctx
        .sql("SELECT * FROM employees WHERE department = 'Marketing' OR department = 'Sales'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+-----------+-------------+---------+-----+------------+",
            "| tenant_id | employee_id | name    | age | department |",
            "+-----------+-------------+---------+-----+------------+",
            "| acme      | 2           | Bob     | 30  | Sales      |",
            "| acme      | 3           | Charlie | 35  | Marketing  |",
            "| globex    | 2           | Eve     | 32  | Sales      |",
            "+-----------+-------------+---------+-----+------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_composite_pk_same_employee_id_different_tenant() {
    // Verify that (acme, 1) and (globex, 1) are treated as different rows.
    // department = 'Engineering' returns both Alice and David who share employee_id=1
    let ctx = setup_composite_pk_test_env().await;

    let results = ctx
        .sql("SELECT * FROM employees WHERE department = 'Engineering'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+-----------+-------------+-------+-----+-------------+",
            "| tenant_id | employee_id | name  | age | department  |",
            "+-----------+-------------+-------+-----+-------------+",
            "| acme      | 1           | Alice | 25  | Engineering |",
            "| globex    | 1           | David | 28  | Engineering |",
            "+-----------+-------------+-------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_composite_pk_complex_nested() {
    // (age <= 25 AND department = 'Engineering') OR department = 'Sales'
    // age <= 25 AND Engineering: Alice(acme,1)
    // Sales: Bob(acme,2), Eve(globex,2)
    // Result: Alice, Bob, Eve
    let ctx = setup_composite_pk_test_env().await;

    let results = ctx
        .sql("SELECT * FROM employees WHERE (age <= 25 AND department = 'Engineering') OR department = 'Sales'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+-----------+-------------+-------+-----+-------------+",
            "| tenant_id | employee_id | name  | age | department  |",
            "+-----------+-------------+-------+-----+-------------+",
            "| acme      | 1           | Alice | 25  | Engineering |",
            "| acme      | 2           | Bob   | 30  | Sales       |",
            "| globex    | 2           | Eve   | 32  | Sales       |",
            "+-----------+-------------+-------+-----+-------------+",
        ],
        &results
    );
}
