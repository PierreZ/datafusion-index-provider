//! Physical plans and operators for index-based data access
//!
//! This module contains:
//! * Index scanning - Generating row IDs from index lookups
//! * Record fetching - Converting index entries to actual records (Removed, handled by Join)
//! * Stream processing - Handling record batches in a streaming fashion

pub mod indexes;
pub mod joins;
