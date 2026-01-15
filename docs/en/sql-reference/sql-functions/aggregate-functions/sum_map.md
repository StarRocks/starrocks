---
displayed_sidebar: docs
---

# sum_map

## Description

Aggregates MAP values by summing numeric values for matching keys across multiple rows. This function is inspired by ClickHouse's sumMap and is particularly useful for data aggregation scenarios involving maps with numeric values.

Given multiple maps with the same key type and numeric value type, `sum_map` merges them by:
1. Collecting all unique keys from all input maps
2. Summing the values for each key across all maps
3. Returning a single map with the aggregated results

## Syntax

```SQL
sum_map(map_expr)
```

## Parameters

- `map_expr`: A MAP expression where the value type must be numeric (TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, or DOUBLE). Both keys and values can be nullable.

## Return value

Returns a MAP with the same key and value types as the input, containing all unique keys and their summed values.

## NULL Handling

The `sum_map` function handles NULL keys and values according to the following rules:

- **NULL values**: Entries with NULL values are **excluded** from the aggregation, regardless of whether the key is NULL or not.
- **NULL keys**: Entries with NULL keys but **non-NULL values** are **included** in the result as `{NULL: aggregated_value}`.
- **NULL maps**: NULL map inputs (i.e., the entire map is NULL) are skipped during aggregation.

**Example behavior:**
```SQL
-- Map entries: {1:10, NULL:5, 2:NULL}
-- After aggregation:
--   - {1:10} is included
--   - {NULL:5} is included (NULL key with non-NULL value)
--   - {2:NULL} is excluded (NULL value)
-- Result: {1:10, NULL:5}
```

## Examples

### Example 1: Basic usage with integer maps

```SQL
CREATE TABLE metrics (
    id INT,
    counters MAP<VARCHAR(10), BIGINT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3;

INSERT INTO metrics VALUES
    (1, map{"clicks":100, "views":200}),
    (2, map{"clicks":50, "impressions":150}),
    (3, map{"views":250, "impressions":350});

SELECT sum_map(counters) FROM metrics;
-- Result: {"clicks":150, "views":450, "impressions":500}
```

### Example 2: Group by aggregation

```SQL
CREATE TABLE daily_stats (
    date DATE,
    category VARCHAR(20),
    metrics MAP<VARCHAR(20), DOUBLE>
) DUPLICATE KEY(date)
DISTRIBUTED BY HASH(date) BUCKETS 3;

INSERT INTO daily_stats VALUES
    ('2024-01-01', 'A', map{"revenue":100.5, "cost":50.2}),
    ('2024-01-01', 'B', map{"revenue":200.3, "cost":80.1}),
    ('2024-01-02', 'A', map{"revenue":150.7, "cost":60.3});

SELECT date, sum_map(metrics) as total_metrics
FROM daily_stats
GROUP BY date
ORDER BY date;

-- Result:
-- 2024-01-01  {"revenue":300.8, "cost":130.3}
-- 2024-01-02  {"revenue":150.7, "cost":60.3}
```

### Example 3: NULL key and value handling

```SQL
-- Create a table with nullable map keys and values
CREATE TABLE events (
    id INT,
    tags MAP<VARCHAR(10), INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3;

-- Insert data with NULL keys and NULL values
INSERT INTO events VALUES
    (1, map{"user":100, "system":50}),
    (2, map{"user":25, NULL:30}),        -- NULL key with non-NULL value
    (3, map{"system":40, "admin":NULL}), -- NULL value (will be excluded)
    (4, map{NULL:15});                   -- NULL key with non-NULL value

SELECT sum_map(tags) FROM events;
-- Result: {"user":125, "system":90, NULL:45}
-- Explanation:
--   - "user": 100 + 25 = 125
--   - "system": 50 + 40 = 90
--   - NULL key: 30 + 15 = 45
--   - "admin":NULL was excluded (NULL value)
```

### Example 4: NULL map handling

```SQL
SELECT sum_map(counters) FROM (
    VALUES 
        (map{1:10, 2:20}),
        (map{1:5, 3:15}),
        (NULL)                -- Entire map is NULL (ignored)
) AS t(counters);
-- Result: {1:15, 2:20, 3:15}
-- NULL maps are ignored in aggregation
```

## Usage notes

- **Empty maps**: Empty maps contribute nothing to the aggregation
- **NULL maps**: NULL map inputs (entire map is NULL) are skipped during aggregation
- **NULL keys**: Entries with NULL keys and non-NULL values are included and aggregated together under a single NULL key in the result
- **NULL values**: Entries with NULL values are excluded from the result, regardless of whether the key is NULL or not
- **Key merging**: Keys that appear in multiple maps have their values summed; keys that appear in only some maps are included with the sum of values from maps where they appear
- **Supported types**: The function only supports maps with numeric value types (TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE)
- **Use cases**: This function is particularly useful for aggregating counters, metrics, or any key-value data where values should be summed

## Keywords

SUM_MAP, MAP, AGGREGATE
