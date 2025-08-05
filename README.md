# DataFusion Index Provider

This crate provides traits and building blocks to add index-based scanning
capabilities to DataFusion `TableProvider`s.

## Core Concepts

-   **`IndexedTableProvider`**: An extension of the DataFusion `TableProvider` trait with methods for discovering and scanning indexes.
-   **`Index`**: A trait representing a physical index on a column that can be scanned to find row IDs.

## How to Use

To add index support to a `TableProvider`, you need to:

1.  **Implement the `Index` trait** for each physical index you want to provide. This trait defines how the index can be scanned with a set of filters to retrieve row IDs.

2.  **Implement the `RecordFetcher` trait** for your `TableProvider`. This trait defines how the execution engine can fetch full data rows using the row IDs returned by an index scan.

3.  **Implement the `IndexedTableProvider` trait** on your `TableProvider`:
    - You must implement the `indexes(&self)` method to return a list of all available indexes for the table.
    - You can then use the default methods of this trait to handle filter analysis and plan creation.

4.  **Update your `TableProvider::scan` method** to delegate to the `IndexedTableProvider`'s helpers:
    - Call `self.analyze_and_optimize_filters(filters)` to identify which filters can be handled by your indexes.
    - If there are indexable filters, call `self.create_execution_plan_with_indexes(...)` to construct an index-aware execution plan (`RecordFetchExec`).
    - If no filters can be handled by an index, fall back to your `TableProvider`'s default full-table scan.

5.  **Update your `TableProvider::supports_filters_pushdown` method** to delegate to `self.supports_filters_index_pushdown(filters)` to correctly report which filters can be pushed down to an index.

## Execution Plans

This crate builds physical execution plans that leverage indexes to accelerate
queries. Depending on the filters, one of the following plans is typically constructed:

### Single Index Scan

For a query with a filter on a single indexed column (e.g., `WHERE col_a = 10`):

```text
+-------------------+
| RecordFetchExec   |
+-------------------+
          |
+-------------------+
| IndexScanExec     |
| (index_on_col_a)  |
+-------------------+
```

### Multiple Index Scans

For a query with filters on multiple indexed columns (e.g., `WHERE col_a = 10 AND col_b > 20`):

```text
+-------------------+
| RecordFetchExec   |
+-------------------+
          |
+-------------------+
| JoinExec          |  (INNER on row_id)
+-------------------+
      /       \
     /         \
+-------------------+   +-------------------+
| IndexScanExec     |   | IndexScanExec     |
| (index_on_col_a)  |   | (index_on_col_b)  |
+-------------------+   +-------------------+
```
