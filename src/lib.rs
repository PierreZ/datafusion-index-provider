//! # DataFusion Index Provider
//!
//! This crate provides traits and building blocks to add index-based scanning
//! capabilities to DataFusion [`datafusion::datasource::TableProvider`]s.
//!
//! ## Core Concepts
//!
//! The core of this library is the [`provider::IndexedTableProvider`] trait. It extends the
//! DataFusion [`datafusion::datasource::TableProvider`] trait with methods for discovering and scanning indexes.
//!
//! To add index support to a data source, you need to:
//!
//! 1.  **Implement the [`physical_plan::Index`] trait:** This trait represents an index on a specific
//!     column. It is responsible for providing its schema, checking if it can evaluate a
//!     filter expression, and, most importantly, performing an index scan to return a
//!     stream of row IDs.
//!
//! 2.  **Implement the [`provider::IndexedTableProvider`] trait:** This trait is implemented on your
//!     `TableProvider`. It is responsible for exposing the available indexes and providing
//!     physical plans for scanning both the indexes and the base table data.
//!
//! ## Usage
//!
//! By implementing these traits, you can leverage the default implementation of
//! [`provider::IndexedTableProvider::scan_with_indexes_or_fallback`], which automatically
//! analyzes filters, routes queries to the appropriate indexes, and combines the results.
//! This method can be called from your `TableProvider::scan` implementation to enable
//! index-based query execution.
//!
//! 3. Use the default implementation of `IndexedTableProvider::scan_with_indexes_or_fallback`
//!    in your `TableProvider::scan` implementation to enable index-based scanning.
//!
//! ## Execution Plans
//!
//! This crate builds physical execution plans that leverage indexes to accelerate
//! queries. Depending on the filters, one of the following plans is typically constructed:
//!
//! ### Single Index Scan
//!
//! For a query with a filter on a single indexed column (e.g., `WHERE col_a = 10`):
//! ```text
//! +-------------------+
//! | RecordFetchExec   |
//! +-------------------+
//!           |
//! +-------------------+
//! | IndexScanExec     |
//! | (index_on_col_a)  |
//! +-------------------+
//! ```
//!
//! ### Multiple Index Scans
//!
//! For a query with filters on multiple indexed columns (e.g., `WHERE col_a = 10 AND col_b > 20`):
//! ```text
//! +-------------------+
//! | RecordFetchExec   |
//! +-------------------+
//!           |
//! +-------------------+
//! | JoinExec          |  (INNER on row_id)
//! +-------------------+
//!       /       \
//!      /         \
//! +-------------------+   +-------------------+
//! | IndexScanExec     |   | IndexScanExec     |
//! | (index_on_col_a)  |   | (index_on_col_b)  |
//! +-------------------+   +-------------------+
//! ```

pub mod physical_plan;
pub mod provider;
pub mod types;
