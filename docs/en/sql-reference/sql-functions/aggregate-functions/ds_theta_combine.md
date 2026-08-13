# ds_theta_combine

Combines multiple serialized Theta sketches into a single serialized sketch. This function is part of the DataSketches Theta family of functions for approximate distinct counting.

`ds_theta_combine` takes a VARBINARY column produced by `ds_theta_accumulate` and merges its rows into a single sketch that represents the union of all distinct values.

It is based on Apache DataSketches. For more information, see [Theta Sketches](https://datasketches.apache.org/docs/Theta/InverseEstimate.html).

## Syntax

```Haskell
sketch ds_theta_combine(sketch)
```

### Parameters

- `sketch`: A VARBINARY column containing serialized Theta sketches created by `ds_theta_accumulate`.

## Return Type

Returns a VARBINARY containing the combined serialized Theta sketch. The `log_k` of the producing `ds_theta_accumulate` is preserved through the merge.

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

-- Create table to store sketches
CREATE TABLE t2 (
  `id` bigint,
  `dt` varchar(10),
  `ds_id` binary,
  `ds_province` binary,
  `ds_age` binary,
  `ds_dt` binary
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 3;

-- Store sketches created by ds_theta_accumulate
INSERT INTO t2 SELECT id, dt,
  ds_theta_accumulate(id),
  ds_theta_accumulate(province, 14),
  ds_theta_accumulate(age),
  ds_theta_accumulate(dt)
FROM t1;

-- Combine sketches grouped by date
SELECT dt,
       ds_theta_combine(ds_id),
       ds_theta_combine(ds_province),
       ds_theta_combine(ds_age),
       ds_theta_combine(ds_dt)
FROM t2
GROUP BY dt
ORDER BY 1
LIMIT 3;
```

## Related Functions

- `ds_theta_accumulate`: Creates serialized Theta sketches from data
- `ds_theta_estimate`: Estimates the distinct count from a serialized sketch
- `ds_theta_count_distinct`: Direct approximate distinct counting function

## Keywords

DS_THETA_COMBINE, THETA, APPROXIMATE, DISTINCT, COUNT, MERGE, UNION
