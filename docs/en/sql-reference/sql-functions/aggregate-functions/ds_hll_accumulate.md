# ds_hll_accumulate

Accumulates values into a HyperLogLog sketch and returns the serialized sketch as a VARBINARY. This function is part of the DataSketches HLL family of functions for approximate distinct counting.

`ds_hll_accumulate` creates a serialized HyperLogLog sketch that can be combined with other sketches using `ds_hll_combine` and estimated using `ds_hll_estimate`.

It is based on Apache DataSketches and provides high precision for approximate distinct counting. For more information, see [HyperLogLog Sketches](https://datasketches.apache.org/docs/HLL/HllSketches.html).

## Syntax

```Haskell
sketch ds_hll_accumulate(expr)
sketch ds_hll_accumulate(expr, log_k)
sketch ds_hll_accumulate(expr, log_k, tgt_type)
```

### Parameters

- `expr`: The expression to accumulate into the sketch. Can be any data type.
- `log_k`: Integer. Range [4, 21]. Default: 17. Controls the precision and memory usage of the sketch.
- `tgt_type`: Valid values are `HLL_4`, `HLL_6` (default) and `HLL_8`. Controls the target type of the HyperLogLog sketch.

## Return Type

Returns a VARBINARY containing the serialized HyperLogLog sketch.

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

-- Basic usage with single argument
SELECT ds_hll_accumulate(id) FROM t1;

-- With custom log_k parameter
SELECT ds_hll_accumulate(province, 20) FROM t1;

-- With both log_k and tgt_type parameters
SELECT ds_hll_accumulate(age, 12, "HLL_6") FROM t1;

-- Group by usage
SELECT dt, 
       ds_hll_accumulate(id), 
       ds_hll_accumulate(province, 20),  
       ds_hll_accumulate(age, 12, "HLL_6"), 
       ds_hll_accumulate(dt) 
FROM t1 
GROUP BY dt 
ORDER BY 1 
LIMIT 3;
```

## Related Functions

- `ds_hll_combine`: Combines multiple serialized sketches into a single sketch
- `ds_hll_estimate`: Estimates the distinct count from a serialized sketch
- `ds_hll_count_distinct`: Direct approximate distinct counting function

## Keywords

ds_HLL_ACCUMULATE, HLL, HYPERLOGLOG, APPROXIMATE, DISTINCT, COUNT
