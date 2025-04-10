---
displayed_sidebar: docs
---

# approx_top_k

## Description

Returns the top `k` most frequently occurring item values in an `expr` along with their approximate counts.

This function is supported from v3.0.

## Syntax

```Haskell
APPROX_TOP_K(<expr> [ , <k> [ , <counter_num> ] ] )
```

## Arguments

- `expr`: An expression of STRING, BOOLEAN, DATE, DATETIME, or numeric type.
- `k`: An optional INTEGER literal greater than 0. If `k` is not specified, it defaults to `5`. The maximum value is `100000`.
- `counter_num`: An optional INTEGER literal greater than or equal to `k`, The larger the `counter_num` is, the more accurate the result will be. However, this also comes with increased CPU and memory costs.

  - The maximum value is `100000`.
  - If `counter_num` is not specified, it defaults to `max(min(2 * k, 100), 100000)`.

## Returns

Results are returned as an ARRAY of type STRUCT, where each STRUCT contains an `item` field for the value (with its original input type) and a `count` field (of type BIGINT) with the approximate number of occurrences. The array is sorted by `count` descending.

The aggregate function returns the top `k` most frequently occurring item values in an expression expr along with their approximate counts. The error in each count may be up to `2.0 * numRows / counter_num` where `numRows` is the total number of rows. Higher values of `counter_num` provide better accuracy at the cost of increased memory usage. Expressions that have fewer than `counter_num` distinct items will yield exact item counts. Results include `NULL` values as their own item in the results.

## Examples

Use data in the [scores](../Window_function.md#window-function-sample-table) table as an example.

```plaintext
-- Calculate the score distribution of each subject.
MySQL > SELECT subject, APPROX_TOP_K(score)  AS top_k FROM scores GROUP BY subject;
+---------+--------------------------------------------------------------------------------------------------------------------+
| subject | top_k                                                                                                              |
+---------+--------------------------------------------------------------------------------------------------------------------+
| physics | [{"item":99,"count":2},{"item":null,"count":1},{"item":100,"count":1},{"item":85,"count":1},{"item":60,"count":1}] |
| english | [{"item":null,"count":1},{"item":92,"count":1},{"item":98,"count":1},{"item":100,"count":1},{"item":85,"count":1}] |
| NULL    | [{"item":90,"count":1}]                                                                                            |
| math    | [{"item":80,"count":2},{"item":null,"count":1},{"item":92,"count":1},{"item":95,"count":1},{"item":70,"count":1}]  |
+---------+--------------------------------------------------------------------------------------------------------------------+

-- Calculate the score distribution of the math subject.
MySQL > SELECT subject, APPROX_TOP_K(score)  AS top_k FROM scores WHERE subject IN  ('math') GROUP BY subject;
+---------+-------------------------------------------------------------------------------------------------------------------+
| subject | top_k                                                                                                             |
+---------+-------------------------------------------------------------------------------------------------------------------+
| math    | [{"item":80,"count":2},{"item":null,"count":1},{"item":95,"count":1},{"item":92,"count":1},{"item":70,"count":1}] |
+---------+-------------------------------------------------------------------------------------------------------------------+
```

## keyword

APPROX_TOP_K
