pub mod employee_provider;
pub mod exec;
pub mod indexes;

use crate::EmployeeTableProvider;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Helper function to setup test environment
pub async fn setup_test_env() -> SessionContext {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .try_init();

    // Create a session context
    let ctx = SessionContext::new();

    // Create and register our employee table
    let provider = EmployeeTableProvider::new();
    ctx.register_table("employees", Arc::new(provider)).unwrap();

    ctx
}
