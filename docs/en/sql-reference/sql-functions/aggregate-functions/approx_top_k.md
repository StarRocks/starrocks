---
displayed_sidebar: "English"
---

# approx_top_k

## Description

Returns the top `k` most frequently occurring item values in an `expr` along with their approximate counts.

## Syntax

```Haskell
APPROX_TOP_K(<expr> [ , <k> [ , <counter_num> ] ] )
```

## Arguments

* `expr`: An expression of STRING, BOOLEAN, DATE, DATETIME, or numeric type.
* `k`: An optional INTEGER literal greater than 0. If `k` is not specified, it defaults to `5`.
    * The maximum value is `100000`
* `counter_num`: An optional INTEGER literal greater than or equal to `k`, The larger the `counter_num` is, the more accurate the result will be. However, this also comes with increased CPU and memory costs. 
    * The maximum value is `100000`
    * If `counter_num` is not specified, it defaults to `max(min(2 * k, 100), 100000)`.

## Returns

Results are returned as an ARRAY of type STRUCT, where each STRUCT contains an `item` field for the value (with its original input type) and a `count` field (of type BIGINT) with the approximate number of occurrences. The array is sorted by `count` descending.

The aggregate function returns the top `k` most frequently occurring item values in an expression expr along with their approximate counts. The error in each count may be up to `2.0 * numRows / counter_num` where `numRows` is the total number of rows. Higher values of `counter_num` provide better accuracy at the cost of increased memory usage. Expressions that have fewer than `counter_num` distinct items will yield exact item counts. Results include `NULL` values as their own item in the results.

## Examples

```plain text
MySQL > SELECT APPROX_TOP_K(L_LINESTATUS) FROM lineitem;
+-------------------------------------------------------------+
| approx_top_k(L_LINESTATUS)                                  |
+-------------------------------------------------------------+
| [{"item":"O","count":3004998},{"item":"F","count":2996217}] |
+-------------------------------------------------------------+

MySQL > SELECT APPROX_TOP_K(L_LINENUMBER) FROM lineitem GROUP BY L_RETURNFLAG
+-------------------------------------------------------------------------------------------------------------------------------------+
| approx_top_k(L_LINENUMBER)                                                                                                          |
+-------------------------------------------------------------------------------------------------------------------------------------+
| [{"item":1,"count":761151},{"item":2,"count":652280},{"item":3,"count":543265},{"item":4,"count":434834},{"item":5,"count":326135}] |
| [{"item":1,"count":368853},{"item":2,"count":316830},{"item":3,"count":263950},{"item":4,"count":211270},{"item":5,"count":158495}] |
| [{"item":1,"count":369996},{"item":2,"count":316718},{"item":3,"count":264179},{"item":4,"count":210911},{"item":5,"count":158657}] |
+-------------------------------------------------------------------------------------------------------------------------------------+
```

## keyword

APPROX_TOP_K

