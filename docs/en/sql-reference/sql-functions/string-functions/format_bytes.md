---
displayed_sidebar: docs
---

# format_bytes

Converts a byte count into a human-readable string with appropriate units (B, KB, MB, GB, TB, PB, EB).

This function is useful for displaying file sizes, table sizes, memory usage, and other storage-related metrics in a user-friendly format. It uses 1024-based calculations (binary prefixes) but displays units as KB, MB, GB for simplicity and familiarity.

This function is supported from v3.4.

## Syntax

```Haskell
VARCHAR format_bytes(BIGINT bytes)
```

## Parameters

- `bytes`: the number of bytes to format. BIGINT is supported. Must be a non-negative integer value.

## Return value

Returns a VARCHAR value representing the formatted byte size with appropriate units.

- For values less than 1024: returns the exact number with "B" (bytes)
- For larger values: returns a formatted string with 2 decimal places and the appropriate unit (KB, MB, GB, TB, PB, EB)
- Returns NULL if the input is negative or NULL

## Usage notes

- The function uses 1024-based (binary) calculations internally (1 KB = 1024 bytes, 1 MB = 1024² bytes, etc.)
- Units are displayed as KB, MB, GB, TB, PB, EB for user familiarity, though they represent binary prefixes (KiB, MiB, GiB, etc.)
- Values greater than or equal to 1 KB are displayed with exactly 2 decimal places
- Byte values (less than 1024) are displayed as whole numbers without decimal places
- Negative values return NULL
- Supports values up to 8 EB (exabytes), covering the full range of BIGINT

## Examples

Example 1: Format various byte sizes.

```sql
SELECT format_bytes(0);
-- 0 B

SELECT format_bytes(123);
-- 123 B

SELECT format_bytes(1024);
-- 1.00 KB

SELECT format_bytes(4096);
-- 4.00 KB

SELECT format_bytes(123456789);
-- 117.74 MB

SELECT format_bytes(10737418240);
-- 10.00 GB
```

Example 2: Handle edge cases and null values.

```sql
SELECT format_bytes(-1);
-- NULL

SELECT format_bytes(NULL);
-- NULL

SELECT format_bytes(0);
-- 0 B
```

Example 3: Practical usage for table size monitoring.

```sql
-- Create a sample table with size information
CREATE TABLE storage_info (
    table_name VARCHAR(64),
    size_bytes BIGINT
);

INSERT INTO storage_info VALUES
('user_profiles', 1073741824),
('transaction_logs', 5368709120),
('product_catalog', 524288000),
('analytics_data', 2199023255552);

-- Display table sizes in human-readable format
SELECT 
    table_name,
    size_bytes,
    format_bytes(size_bytes) as formatted_size
FROM storage_info
ORDER BY size_bytes DESC;

-- Expected output:
-- +------------------+---------------+----------------+
-- | table_name       | size_bytes    | formatted_size |
-- +------------------+---------------+----------------+
-- | analytics_data   | 2199023255552 | 2.00 TB        |
-- | transaction_logs | 5368709120    | 5.00 GB        |
-- | user_profiles    | 1073741824    | 1.00 GB        |
-- | product_catalog  | 524288000     | 500.00 MB      |
-- +------------------+---------------+----------------+
```

Example 4: Precision and rounding behavior.

```sql
SELECT format_bytes(1536);       -- 1.50 KB (exactly 1.5 KB)
SELECT format_bytes(1025);       -- 1.00 KB (rounded to 2 decimals)
SELECT format_bytes(1048576 + 52429);  -- 1.05 MB (rounded to 2 decimals)
```

Example 5: Full range of supported units.

```sql
SELECT format_bytes(512);                    -- 512 B
SELECT format_bytes(2048);                   -- 2.00 KB  
SELECT format_bytes(1572864);                -- 1.50 MB
SELECT format_bytes(2147483648);             -- 2.00 GB
SELECT format_bytes(1099511627776);          -- 1.00 TB
SELECT format_bytes(1125899906842624);       -- 1.00 PB
SELECT format_bytes(1152921504606846976);    -- 1.00 EB
```

## keyword

FORMAT_BYTES