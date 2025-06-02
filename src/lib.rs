//! DataFusion Index Provider implementation.
//!
//! This crate provides an extension to DataFusion that adds support for indexed column lookups,
//! allowing for more efficient query execution when indexes are available.
//!
//! The main components are:
//! * [`optimizer`] - Module containing query optimization logic for index operations
//! * [`physical`] - Module containing physical execution components for index operations
//! * [`provider`] - Module containing provider implementation

//! Column name used to store internal Row IDs for joining index results.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
pub const ROW_ID_COLUMN_NAME: &str = "__row_id__";

/// Creates the standard schema for index scan results (a single __row_id column)
/// with the specified data type.
pub fn index_scan_schema(data_type: DataType) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        ROW_ID_COLUMN_NAME,
        data_type,
        false, // Typically, row IDs are not nullable
    )]))
}

pub mod optimizer;
pub mod physical;
pub mod provider;
pub mod record_fetch;
