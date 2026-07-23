# ds_theta_estimate

Estimates the approximate distinct count from a serialized Theta sketch. This function is part of the DataSketches Theta family of functions for approximate distinct counting.

`ds_theta_estimate` takes a VARBINARY serialized sketch created by `ds_theta_accumulate` or `ds_theta_combine` and returns the estimated number of distinct values.

It is based on Apache DataSketches. For more information, see [Theta Sketches](https://datasketches.apache.org/docs/Theta/InverseEstimate.html).

## Syntax

```Haskell
bigint ds_theta_estimate(sketch)
```

### Parameters

- `sketch`: A VARBINARY column containing serialized Theta sketches created by `ds_theta_accumulate` or `ds_theta_combine`.

## Return Type

Returns a BIGINT representing the estimated distinct count.

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

-- Estimate distinct counts grouped by date
SELECT dt,
       ds_theta_estimate(ds_id),
       ds_theta_estimate(ds_province),
       ds_theta_estimate(ds_age),
       ds_theta_estimate(ds_dt)
FROM t2
GROUP BY dt
ORDER BY 1
LIMIT 3;
```

## Related Functions

- `ds_theta_accumulate`: Creates serialized Theta sketches from data
- `ds_theta_combine`: Combines multiple serialized sketches into a single sketch
- `ds_theta_count_distinct`: Direct approximate distinct counting function

## Keywords

DS_THETA_ESTIMATE, THETA, APPROXIMATE, DISTINCT, COUNT, ESTIMATE
