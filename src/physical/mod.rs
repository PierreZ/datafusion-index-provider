//! Physical execution components for index operations.
//!
//! This module contains the physical execution components needed to perform
//! index-based operations in DataFusion, including:
//!
//! * Record fetching - Converting index entries to actual records
//! * Stream processing - Handling record batches in a streaming fashion

pub mod joins;
pub mod record_fetch;
