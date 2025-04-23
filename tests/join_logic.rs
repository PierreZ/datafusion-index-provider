//! Tests for the join selection logic

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_index_provider::physical::joins::try_create_lookup_join;

use crate::common::exec::IndexLookupExec;

mod common;

/// Creates the simple schema used by IndexLookupExec (just "index" column).
fn create_index_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "index",
        DataType::UInt64,
        false,
    )]))
}

#[test]
fn test_uses_sort_merge_join() {
    let schema = create_index_schema();
    let dummy_indices = vec![1, 3, 5];

    // Create two IndexLookupExec plans that *report* sorted output
    let left_sorted = Arc::new(IndexLookupExec::new_sorted(
        schema.clone(),
        dummy_indices.clone(),
    )) as Arc<dyn ExecutionPlan>;
    let right_sorted =
        Arc::new(IndexLookupExec::new_sorted(schema, dummy_indices)) as Arc<dyn ExecutionPlan>;

    // Call the join logic function
    let result_plan = try_create_lookup_join(left_sorted, right_sorted).unwrap();

    // Assert that the result is a SortMergeJoinExec
    assert!(
        result_plan.as_any().is::<SortMergeJoinExec>(),
        "Expected SortMergeJoinExec for sorted inputs"
    );
}

#[test]
fn test_uses_hash_join() {
    let schema = create_index_schema();
    let dummy_indices = vec![5, 1, 3]; // Unsorted indices

    // Create two standard IndexLookupExec plans (which report unsorted output)
    let left_unsorted = Arc::new(IndexLookupExec::new(schema.clone(), dummy_indices.clone()))
        as Arc<dyn ExecutionPlan>;
    let right_unsorted =
        Arc::new(IndexLookupExec::new(schema, dummy_indices)) as Arc<dyn ExecutionPlan>;

    // Call the join logic function
    let result_plan = try_create_lookup_join(left_unsorted, right_unsorted).unwrap();

    // Assert that the result is a HashJoinExec
    assert!(
        result_plan.as_any().is::<HashJoinExec>(),
        "Expected HashJoinExec for unsorted inputs"
    );
}
