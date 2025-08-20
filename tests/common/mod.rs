pub mod age_index;
pub mod department_index;
pub mod employee_provider;
pub mod record_fetcher;

use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::execution::context::SessionContext;
use employee_provider::EmployeeTableProvider;
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
            "Name {expected_name} not found in {names:?}"
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
            "Age {expected_age} not found in {ages:?}"
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

pub fn assert_departments(results: &[RecordBatch], expected_departments: &[&str]) {
    let mut departments = HashSet::new();
    for batch in results {
        let dept_batch = batch
            .column(3) // department is third in projection
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for dept in dept_batch.iter().flatten() {
            departments.insert(dept.to_string());
        }
    }

    for expected_dept in expected_departments {
        assert!(
            departments.contains(*expected_dept),
            "Department {expected_dept} not found in {departments:?}"
        );
    }
    assert_eq!(
        departments.len(),
        expected_departments.len(),
        "Expected {} departments: {:?}, got {} departments: {:?}",
        expected_departments.len(),
        expected_departments,
        departments.len(),
        departments
    );
}

pub fn extract_names(results: &[RecordBatch]) -> Vec<String> {
    let mut names = Vec::new();
    for batch in results {
        let name_batch = batch
            .column(1) // name is first in projection
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for name in name_batch.iter().flatten() {
            names.push(name.to_string());
        }
    }
    names.sort();
    names
}
