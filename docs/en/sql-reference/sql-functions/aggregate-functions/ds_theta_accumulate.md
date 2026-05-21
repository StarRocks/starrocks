# ds_theta_accumulate

Accumulates values into a Theta sketch and returns the serialized sketch as a VARBINARY. This function is part of the DataSketches Theta family of functions for approximate distinct counting.

`ds_theta_accumulate` creates a serialized Theta sketch that can be combined with other sketches using `ds_theta_combine` and estimated using `ds_theta_estimate`.

It is based on Apache DataSketches. For more information, see [Theta Sketches](https://datasketches.apache.org/docs/Theta/InverseEstimate.html).

## Syntax

```Haskell
sketch ds_theta_accumulate(expr)
sketch ds_theta_accumulate(expr, log_k)
```

### Parameters

- `expr`: The expression to accumulate into the sketch. Can be any data type.
- `log_k`: Integer. Range [5, 26]. Default: 12. Controls the precision and memory usage of the sketch.

## Return Type

Returns a VARBINARY containing the serialized Theta sketch. The serialized blob also carries the `log_k` value so it survives across nodes, agg-state materializations, and downstream `ds_theta_combine` / `ds_theta_estimate` calls.

## Examples

```sql
-- Create test table
CREATE TABLE t1 (
  id BIGINT,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10)
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3;

-- Insert test data
INSERT INTO t1 SELECT generate_series, generate_series, generate_series % 100, "2024-07-24"
FROM table(generate_series(1, 1000));

-- Basic usage with single argument (default log_k = 12)
SELECT ds_theta_accumulate(id) FROM t1;

-- With custom log_k for higher precision
SELECT ds_theta_accumulate(province, 14) FROM t1;

-- Group by usage
SELECT dt,
       ds_theta_accumulate(id),
       ds_theta_accumulate(province, 14),
       ds_theta_accumulate(age),
       ds_theta_accumulate(dt)
FROM t1
GROUP BY dt
ORDER BY 1
LIMIT 3;
```

## Related Functions

- `ds_theta_combine`: Combines multiple serialized sketches into a single sketch
- `ds_theta_estimate`: Estimates the distinct count from a serialized sketch
- `ds_theta_count_distinct`: Direct approximate distinct counting function

## Keywords

DS_THETA_ACCUMULATE, THETA, APPROXIMATE, DISTINCT, COUNT
