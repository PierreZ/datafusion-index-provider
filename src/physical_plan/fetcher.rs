//! Defines the [`RecordFetcher`] trait for fetching data based on row IDs.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::Result;

/// A trait for fetching full data records based on a batch of row IDs from an index.
///
/// This trait is used by the `RecordFetchExec` physical operator to abstract
/// the process of retrieving data from the underlying storage. An implementation
/// of `RecordFetcher` knows how to take a `RecordBatch` containing row
/// identifiers and return the corresponding full data rows.
#[async_trait]
pub trait RecordFetcher: Send + Sync + std::fmt::Debug {
    /// Returns the schema of the records that will be fetched.
    ///
    /// This schema should match the schema of the `RecordBatch` returned by [`Self::fetch`].
    fn schema(&self) -> SchemaRef;

    /// Fetches a batch of data records corresponding to the row IDs in `index_batch`.
    ///
    /// # Arguments
    /// * `index_batch` - A `RecordBatch` containing a single column of row IDs
    ///   that identify the records to be fetched.
    ///
    /// The implementor is responsible for fetching the corresponding data and returning
    /// it as a `RecordBatch` with a schema matching [`Self::schema()`].
    async fn fetch(&self, index_batch: RecordBatch) -> Result<RecordBatch>;
}
