# Performance Optimization

Big Data pipelines require conscious optimization strategies to prevent ballooning compute costs and missed SLAs. This pipeline utilizes several key PySpark and Delta Lake optimizations.

## 1. Incremental Processing (Watermarking)
Instead of processing a massive dataset daily, the pipeline identifies and processes *only* the new records that have arrived since the last successful run. This drastically reduces read/write times and cluster compute usage.

## 2. Delta Lake OPTIMIZE & Z-ORDER
Delta Lake files can suffer from the "small file problem" due to frequent incremental loads. To counteract this:
- The `OPTIMIZE` command is run at the end of the Silver layer processing to compact small Parquet files into larger, optimally sized files.
- We utilize `ZORDER BY (region, category, order_date)` during optimization. Z-Ordering physically co-locates related data in the storage layer. Subsequent queries targeting specific regions or categories will execute significantly faster due to enhanced Data Skipping.

## 3. Predicate Pushdown (Partitioning)
The Silver and Gold tables are partitioned logically based on expected query patterns:
- Silver: `partitionBy("order_year", "order_month")`
- Gold (Daily Dashboard): `partitionBy("order_date")`
This allows the query engine to ignore massive amounts of irrelevant data right at the storage level.

## 4. DataFrame Caching (Removed in refactoring but supported)
While caching (`df.cache()`) was demonstrated in the original training notebooks for repeated queries, it has been intentionally omitted from the modular ETL script to preserve cluster memory, as data is written immediately to Delta tables. However, caching remains highly relevant for BI tools directly querying the Gold layer via Databricks SQL endpoints.
