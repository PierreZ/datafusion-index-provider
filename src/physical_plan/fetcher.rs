use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::Result;

/// Fetches full data records based on a batch of row IDs from an index.
#[async_trait]
pub trait RecordFetcher: Send + Sync + std::fmt::Debug {
    /// Returns the schema of the records that will be fetched.
    fn schema(&self) -> SchemaRef;

    /// Fetches a batch of data records corresponding to the row IDs in `index_batch`.
    ///
    /// The `index_batch` is guaranteed to contain a single column of row IDs.
    /// The implementor is responsible for fetching the corresponding data and returning
    /// it as a `RecordBatch` with the correct table schema.
    async fn fetch(&self, index_batch: RecordBatch) -> Result<RecordBatch>;
}
