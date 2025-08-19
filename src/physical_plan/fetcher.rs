//! Defines the [`RecordFetcher`] trait for fetching complete records using row IDs.
//!
//! The [`RecordFetcher`] trait is a key abstraction in the two-phase execution model.
//! After indexes produce row IDs during the index phase, the fetch phase uses this
//! trait to retrieve the actual data records corresponding to those row IDs.

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;

/// A trait for fetching complete data records based on row IDs produced by index scans.
///
/// This trait abstracts the process of retrieving actual data records from the underlying
/// storage system using row identifiers. It serves as the bridge between the index phase
/// (which produces row IDs) and the final query results (which contain complete records).
///
/// ## Implementation Requirements
///
/// Implementations must handle:
/// - **Row ID extraction**: Parse the `__row_id__` column from the input batch
/// - **Efficient lookup**: Retrieve records using the most efficient access pattern for your storage
/// - **Schema consistency**: Return records matching the schema from `schema()`
/// - **Error handling**: Properly propagate storage errors and handle missing records
/// - **Async execution**: Support concurrent fetching for performance
///
/// ## Performance Considerations
///
/// The performance of your `RecordFetcher` implementation directly impacts query performance:
/// - **Batch processing**: Process multiple row IDs together to amortize lookup costs
/// - **Storage locality**: Consider sorting row IDs to improve storage access patterns  
/// - **Caching**: Implement appropriate caching strategies for frequently accessed data
/// - **Resource management**: Manage memory and connection pooling efficiently
#[async_trait]
pub trait RecordFetcher: Send + Sync + std::fmt::Debug {
    /// Returns the schema of the complete records that will be fetched.
    ///
    /// This schema represents the full table structure and must match the schema of
    /// the `RecordBatch` returned by `fetch()`. It typically contains all columns
    /// from your table, not just the row ID column.
    fn schema(&self) -> SchemaRef;

    /// Fetches complete data records corresponding to the row IDs in the input batch.
    ///
    /// This method receives a batch containing row IDs (in the `__row_id__` column)
    /// and must return the complete records for those IDs. The order of output records
    /// should correspond to the order of input row IDs.
    ///
    /// # Arguments
    /// * `index_batch` - A `RecordBatch` containing row IDs to fetch. This batch has
    ///   a single column named `__row_id__` containing the identifiers.
    ///
    /// # Returns
    /// A `RecordBatch` containing the complete records with schema matching `schema()`.
    /// The number of output rows should equal the number of input row IDs unless some
    /// records are missing (which may indicate data consistency issues).
    ///
    /// # Error Handling
    /// Return errors for:
    /// - Storage system failures (connection errors, timeouts)
    /// - Invalid row IDs or data corruption
    /// - Schema mismatches between expected and actual data
    async fn fetch(&self, index_batch: RecordBatch) -> Result<RecordBatch>;
}
