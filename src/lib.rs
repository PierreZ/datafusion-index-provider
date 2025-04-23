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
pub const ROW_ID_COLUMN_NAME: &str = "__row_id__";

pub mod optimizer;
pub mod physical;
pub mod provider;
