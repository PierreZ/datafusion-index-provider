pub mod age_index;
pub mod department_index;
pub mod employee_provider;
pub mod exec;
pub mod record_fetcher;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::execution::context::SessionContext;
use employee_provider::EmployeeTableProvider;
use env_logger;
use log;
use std::collections::HashSet;
use std::sync::Arc;

/// Helper function to setup test environment
pub async fn setup_test_env() -> SessionContext {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .try_init();

    let ctx = SessionContext::new();

    let provider = EmployeeTableProvider::default();
    ctx.register_table("employees", Arc::new(provider)).unwrap();

    ctx
}

pub fn assert_names(results: &[RecordBatch], expected_names: &[&str]) {
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

pub fn assert_ages(results: &[RecordBatch], expected_ages: &[i32]) {
    let mut ages = HashSet::new();
    for batch in results {
        let age_batch = batch
            .column(2) // age is second in projection
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for age in age_batch.iter().flatten() {
            ages.insert(age.to_string());
        }
    }

    for expected_age in expected_ages {
        assert!(
            ages.contains(&expected_age.to_string()),
            "Age {} not found in {:?}",
            expected_age,
            ages
        );
    }
    assert_eq!(
        ages.len(),
        expected_ages.len(),
        "Expected {} ages: {:?}, got {} ages: {:?}",
        expected_ages.len(),
        expected_ages,
        ages.len(),
        ages
    );
}
