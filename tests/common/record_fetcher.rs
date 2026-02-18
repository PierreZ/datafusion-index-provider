use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int32Array, UInt64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion_index_provider::physical_plan::fetcher::RecordFetcher;

/// Mapper that filters batches using index results
pub struct BatchMapper {
    batches: Vec<RecordBatch>,
}

impl BatchMapper {
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches }
    }
}

impl fmt::Debug for BatchMapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BatchMapper")
    }
}

#[async_trait]
impl RecordFetcher for BatchMapper {
    fn schema(&self) -> SchemaRef {
        self.batches
            .first()
            .expect("BatchMapper requires at least one batch")
            .schema()
    }

    async fn fetch(&self, index_batch: RecordBatch) -> Result<RecordBatch> {
        log::debug!("Index batch: {index_batch:?}");
        let indices = index_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let row_ids: Vec<usize> = indices.iter().flatten().map(|i| i as usize).collect();

        log::debug!("Row ids: {row_ids:?}");

        apply_row_filter(&self.batches[0], &row_ids)
    }
}

fn apply_row_filter(batch: &RecordBatch, row_ids: &[usize]) -> Result<RecordBatch> {
    log::debug!("Row ids: {row_ids:?}");
    let new_columns: Result<Vec<Arc<dyn Array>>> = batch
        .columns()
        .iter()
        .map(|col| {
            Ok(Arc::new(datafusion::arrow::compute::take(
                col.as_ref(),
                &Int32Array::from_iter_values(row_ids.iter().map(|&i| (i - 1) as i32)),
                None,
            )?) as Arc<dyn Array>)
        })
        .collect();

    log::debug!("New columns: {new_columns:?}");

    Ok(RecordBatch::try_new(batch.schema(), new_columns?)?)
}
