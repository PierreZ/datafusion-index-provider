use arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use futures_core::Stream;
use futures_util::StreamExt;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct ScanWithIndexStream {
    /// Input execution plan
    input: Option<SendableRecordBatchStream>,
    /// Copy of the index schema
    index_schema: SchemaRef,
    /// Execution time metrics
    baseline_metrics: BaselineMetrics,
}

impl ScanWithIndexStream {
    pub fn new(input: SendableRecordBatchStream, baseline_metrics: BaselineMetrics) -> Self {
        let schema = input.schema();
        Self {
            input: Some(input),
            index_schema: schema,
            baseline_metrics,
        }
    }
    fn stream_scan(&mut self, batch: RecordBatch) -> Option<RecordBatch> {
        unimplemented!()
    }
}

impl Stream for ScanWithIndexStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = match &mut self.input {
            // input has been cleared
            None => Poll::Ready(None),
            Some(input) => {
                let poll = input.poll_next_unpin(cx);

                poll.map(|item| match item {
                    Some(Ok(batch)) => Ok(self.stream_scan(batch)).transpose(),
                    other => other,
                })
            }
        };
        self.baseline_metrics.record_poll(poll)
    }
}