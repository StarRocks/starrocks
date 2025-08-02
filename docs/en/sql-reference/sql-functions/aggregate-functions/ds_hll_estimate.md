# ds_hll_estimate

Estimates the approximate distinct count from a serialized HyperLogLog sketch. This function is part of the DataSketches HLL family of functions for approximate distinct counting.

`ds_hll_estimate` takes a VARBINARY serialized sketch created by `ds_hll_accumulate` or `ds_hll_combine` and returns the estimated number of distinct values.

It is based on Apache DataSketches and provides high precision for approximate distinct counting. For more information, see [HyperLogLog Sketches](https://datasketches.apache.org/docs/HLL/HllSketches.html).

## Syntax

```Haskell
bigint ds_hll_estimate(sketch)
```

### Parameters

- `sketch`: A VARBINARY column containing serialized HyperLogLog sketches created by `ds_hll_accumulate` or `ds_hll_combine`.

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

-- Store sketches created by ds_hll_accumulate
INSERT INTO t2 SELECT id, dt,
  ds_hll_accumulate(id), 
  ds_hll_accumulate(province, 10), 
  ds_hll_accumulate(age, 20, "HLL_6"), 
  ds_hll_accumulate(dt, 10, "HLL_8") 
FROM t1;

-- Estimate distinct counts grouped by date
SELECT dt, 
       ds_hll_estimate(ds_id), 
       ds_hll_estimate(ds_province), 
       ds_hll_estimate(ds_age), 
       ds_hll_estimate(ds_dt) 
FROM t2 
GROUP BY dt 
ORDER BY 1 
LIMIT 3;
```

## Related Functions

- `ds_hll_accumulate`: Creates serialized HyperLogLog sketches from data
- `ds_hll_combine`: Combines multiple serialized sketches into a single sketch
- `ds_hll_count_distinct`: Direct approximate distinct counting function

## Keywords

ds_HLL_ESTIMATE, HLL, HYPERLOGLOG, APPROXIMATE, DISTINCT, COUNT, ESTIMATE 