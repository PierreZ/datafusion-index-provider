use std::sync::Arc;

use arrow::array::{Array, Int32Array, UInt64Array};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::Result;

use datafusion_index_provider::record_fetch::RecordFetcher;

/// Mapper that filters batches using index results
pub struct BatchMapper {
    batches: Vec<RecordBatch>,
}

impl BatchMapper {
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches }
    }
}

#[async_trait]
impl RecordFetcher for BatchMapper {
    async fn fetch_record(&mut self, index_batch: RecordBatch) -> Result<RecordBatch> {
        log::debug!("Index batch: {:?}", index_batch);
        let indices = index_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let row_ids: Vec<usize> = indices.iter().flatten().map(|i| i as usize).collect();

        log::debug!("Row ids: {:?}", row_ids);

        apply_row_filter(&self.batches[0], &row_ids)
    }
}

fn apply_row_filter(batch: &RecordBatch, row_ids: &[usize]) -> Result<RecordBatch> {
    log::debug!("Row ids: {:?}", row_ids);
    let new_columns: Result<Vec<Arc<dyn Array>>> = batch
        .columns()
        .iter()
        .map(|col| {
            Ok(Arc::new(arrow::compute::take(
                col.as_ref(),
                &Int32Array::from_iter_values(row_ids.iter().map(|&i| i as i32)),
                None,
            )?) as Arc<dyn Array>)
        })
        .collect();

    Ok(RecordBatch::try_new(batch.schema(), new_columns?)?)
}
