# DataFusion Index Provider

This crate provides traits and building blocks to add index-based scanning
capabilities to DataFusion `TableProvider`s.

## Core Concepts

-   **`IndexedTableProvider`**: An extension of the DataFusion `TableProvider` trait with methods for discovering and scanning indexes.
-   **`Index`**: A trait representing a physical index on a column that can be scanned to find row IDs.

## How to Use

To add index support to a data source, you need to:

1.  Implement the `Index` trait for each index you want to provide. This trait defines how to scan the index to retrieve row IDs based on filter predicates.
2.  Implement the `IndexedTableProvider` trait for your `TableProvider`. This trait analyzes filters, routes queries to the appropriate indexes, and combines the results.
3.  Use the default implementation `IndexedTableProvider` in the `TableProvider`:
    - [`IndexedTableProvider::scan_with_indexes_or_fallback`](IndexedTableProvider::scan_with_indexes_or_fallback) in your `TableProvider::scan` implementation to enable index-based scanning.
    - [`IndexedTableProvider::supports_filters_index_pushdown`](IndexedTableProvider::supports_filters_index_pushdown) in your `TableProvider::supports_filters_pushdown` implementation to enable index pushdown.
4. Implement the `RecordFetcher` trait for your `TableProvider`. This trait is used by the `RecordFetchExec` physical operator to abstract the process of retrieving data from the underlying storage from the row IDs.

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
