use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion_common::Result;
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};

/// A trait for fetching records based on index entries.
/// Implementors of this trait are responsible for converting index entries into actual data records.
#[async_trait]
pub trait RecordFetcher: Send + Sync {
    /// Fetches actual records based on the provided index batch.
    ///
    /// # Arguments
    /// * `index_batch` - A RecordBatch containing index entries that point to actual records
    ///
    /// # Returns
    /// A RecordBatch containing the fetched records
    async fn fetch_record(&mut self, index_batch: RecordBatch) -> Result<RecordBatch>;
}

/// A stream that fetches records based on index entries.
/// This stream takes an input stream of index entries and uses a RecordFetcher
/// to convert those entries into actual data records.
pub struct RecordFetchStream {
    input: SendableRecordBatchStream, // Input stream providing index batches
    output_schema: SchemaRef,         // Schema of the RecordBatches yielded by this stream
    baseline_metrics: BaselineMetrics,
    fetcher: Box<dyn RecordFetcher>, // Logic to fetch data based on index batch
    /// Flag to prevent polling input after it returns None or an error
    input_ended: bool,
}

impl RecordFetchStream {
    /// Creates a new RecordFetchStream.
    pub fn new(
        input: SendableRecordBatchStream,
        output_schema: SchemaRef,         // Schema of the final output
        partition: usize,                 // Partition index for metrics
        fetcher: Box<dyn RecordFetcher>,  // The fetcher implementation
        metrics: ExecutionPlanMetricsSet, // Parent metrics set
    ) -> Self {
        let baseline_metrics = BaselineMetrics::new(&metrics, partition);
        Self {
            input,
            output_schema, // Store the passed output schema
            baseline_metrics,
            fetcher,
            input_ended: false,
        }
    }
}

impl Stream for RecordFetchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.input_ended {
            return Poll::Ready(None);
        }

        // Poll the input stream
        let poll = match self.input.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                self.input_ended = true; // Mark input as ended
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(e))) => {
                self.input_ended = true; // Stop polling on error
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(Some(Ok(index_batch))) => {
                // Got an index batch, now poll the fetcher
                match self.fetcher.fetch_record(index_batch).poll_unpin(cx) {
                    Poll::Ready(fetch_result) => Poll::Ready(Some(fetch_result)),
                    Poll::Pending => Poll::Pending, // Fetcher is not ready yet
                }
            }
        };
        // Record metrics based on the poll result
        self.baseline_metrics.record_poll(poll)
    }
}

impl RecordBatchStream for RecordFetchStream {
    /// Returns the schema of the RecordBatches produced by this stream.
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone() // Return the correct output schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, UInt8Array, UInt8Builder};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::memory::MemoryStream;
    use std::collections::HashMap;
    use std::sync::Arc;

    struct Mapper {
        database: HashMap<u8, u8>,
    }

    impl Mapper {
        pub fn new_and_init(database: HashMap<u8, u8>) -> Self {
            Self { database }
        }
    }

    #[async_trait]
    impl RecordFetcher for Mapper {
        async fn fetch_record(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
            let column = batch.column_by_name("id").expect("should find column id");
            let index_entry = column.as_any().downcast_ref::<UInt8Array>().unwrap();
            let index_entry: Vec<u8> = index_entry.iter().flatten().collect();

            let mut ids = Vec::with_capacity(batch.num_rows());
            let mut records = Vec::with_capacity(batch.num_rows());

            for index in index_entry {
                match self.database.get(&index) {
                    None => (),
                    Some(value) => {
                        ids.push(index);
                        records.push(*value);
                    }
                }
            }

            let schema = Schema::new(vec![
                Field::new("id", DataType::UInt8, false),
                Field::new("account_balance", DataType::UInt8, false),
            ]);

            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![
                    Arc::new(UInt8Array::from(ids)),
                    Arc::new(UInt8Array::from(records)),
                ],
            )?;

            Ok(batch)
        }
    }

    fn create_index_memory_stream(ids: Vec<u8>) -> Result<MemoryStream> {
        let schema = Schema::new(vec![Field::new("id", DataType::UInt8, false)]);

        let mut id_array = UInt8Builder::with_capacity(ids.len());
        for id in ids {
            id_array.append_value(id);
        }

        MemoryStream::try_new(
            vec![RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![Arc::new(id_array.finish())],
            )?],
            Arc::new(schema),
            None,
        )
    }

    #[tokio::test]
    async fn single_index() {
        let stream = Box::pin(create_index_memory_stream(vec![1, 2]).unwrap());

        let mut mapper = RecordFetchStream::new(
            stream,
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt8, false),
                Field::new("account_balance", DataType::UInt8, false),
            ])),
            0,
            Box::new(Mapper::new_and_init(HashMap::from([(1, 42), (2, 43)]))),
            ExecutionPlanMetricsSet::new(),
        );

        let records = mapper
            .next()
            .await
            .expect("Should be allowed to poll on first try")
            .expect("should contain a RecordBatch");
        assert_eq!(2, records.num_rows(), "Bad number of rows");

        let column = records
            .column_by_name("account_balance")
            .expect("cannot retrieve column 'account_balance'");
        let data = column
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("could to cast to u8");
        let projected_data: Vec<u8> = data.iter().flatten().collect();

        assert_eq!(
            42,
            projected_data.first().expect("should find index 0").clone()
        );
        assert_eq!(
            43,
            projected_data.get(1).expect("should find index 1").clone()
        );
    }
}
