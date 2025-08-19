# DataFusion Index Provider

A Rust crate that adds index-based query acceleration to [DataFusion](https://github.com/apache/datafusion) `TableProvider`s. Dramatically improve query performance by leveraging secondary indexes to reduce I/O for selective queries.

## Quick Start

Add index support to your DataFusion table in 3 steps:

```rust
use datafusion_index_provider::{IndexedTableProvider, Index, RecordFetcher};

// 1. Implement the Index trait for your indexes
impl Index for MyIndex {
    fn name(&self) -> &str { "my_index" }
    fn column_name(&self) -> &str { "indexed_column" }
    fn scan(&self, filters: &[Expr], limit: Option<usize>) -> Result<SendableRecordBatchStream> {
        // Return row IDs that match the filters
    }
    // ... other methods
}

// 2. Implement RecordFetcher for your table
#[async_trait]
impl RecordFetcher for MyTable {
    async fn fetch(&self, index_batch: RecordBatch) -> Result<RecordBatch> {
        // Fetch full records using the row IDs from index_batch
    }
}

// 3. Add IndexedTableProvider to your TableProvider
impl IndexedTableProvider for MyTable {
    fn indexes(&self) -> Result<Vec<Arc<dyn Index>>> {
        Ok(vec![self.my_index.clone()])
    }
}

// Use in your TableProvider::scan method
impl TableProvider for MyTable {
    async fn scan(/* ... */) -> Result<Arc<dyn ExecutionPlan>> {
        let (indexable, remaining) = self.analyze_and_optimize_filters(filters)?;
        
        if !indexable.is_empty() {
            // Index-accelerated query
            self.create_execution_plan_with_indexes(&indexable, /* ... */).await
        } else {
            // Regular full table scan
            self.create_full_scan_plan(/* ... */).await
        }
    }
}
```

## Supported Query Types

The system automatically optimizes queries with complex filter expressions:

```sql
-- Single index lookups
SELECT * FROM table WHERE indexed_col = 'value';

-- Multi-index AND queries  
SELECT * FROM table WHERE col_a = 1 AND col_b > 10;

-- Multi-index OR queries (with automatic deduplication)
SELECT * FROM table WHERE col_a = 1 OR col_b = 2;

-- Complex nested conditions
SELECT * FROM table WHERE (col_a = 1 AND col_b = 2) OR (col_a = 3 AND col_b = 4);
```

## Architecture

The crate uses a two-phase execution model:

1. **Index Phase**: Scan indexes to find row IDs matching your query filters
2. **Fetch Phase**: Use row IDs to fetch complete records from storage

This approach minimizes I/O by only reading the rows you actually need.

## Key Features

- **Automatic query optimization**: Analyzes filter expressions and routes to appropriate indexes
- **Complex query support**: Handles arbitrarily nested AND/OR conditions 
- **Deduplication**: Automatically handles overlapping results from OR queries
- **Streaming execution**: Memory-efficient streaming through the entire pipeline
- **Fallback support**: Gracefully falls back to full table scans when indexes can't help

## Performance Benefits

Best results with **selective queries** (< 10% of total rows):

- **High selectivity**: 10-100x performance improvement typical
- **Medium selectivity**: 2-10x improvement depending on index efficiency  
- **Low selectivity**: May fall back to full table scan automatically

## Documentation

See the [crate documentation](https://docs.rs/datafusion-index-provider) for comprehensive implementation guides, performance characteristics, and advanced usage patterns.
