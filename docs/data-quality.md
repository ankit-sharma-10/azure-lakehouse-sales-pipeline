# Data Quality & Validation

The pipeline enforces data quality rules strictly at the **Silver Layer** (cleansing layer). The Bronze layer is intentionally kept as raw as possible to ensure no source data is prematurely dropped or corrupted during ingestion.

## Validation Rules

### 1. NULL Handling
- `customer_id` is coerced to `"UNKNOWN"` if missing.
- `category` is coerced to `"Uncategorized"` if missing.
- `region` is coerced to `"Unknown"` if missing.
- `status` is coerced to `"Unknown"` if missing.

### 2. Deduplication
- Duplicates are handled using PySpark Window functions.
- The pipeline partitions data by `order_id` and orders by `ingestion_timestamp` descending.
- Only the latest record (where `row_number() == 1`) is retained, discarding older duplicates.

### 3. Business Rule Validation
- `quantity` must be strictly greater than `0`.
- `unit_price` must be strictly greater than `0`.
- Records failing these checks are filtered out before insertion into the Silver table.

### 4. Data Type Enforcement
- All monetary and quantity values are explicitly cast to `DoubleType` and `IntegerType` respectively.
- Dates are strictly parsed and cast to `DateType`.

### 5. String Standardization
- Categorical string columns (`category`, `region`, `status`) are `trim()`med of whitespace and converted to `upper()` case to prevent aggregation errors in the Gold layer (e.g., separating "electronics" and "Electronics").
