---
displayed_sidebar: docs
---

# materialize

## Description

Returns the input value unchanged but acts as an optimization barrier for the FE (Frontend) optimizer. Wrapping an expression with `materialize()` prevents the optimizer from applying transformations such as constant folding, partition pruning, and other rewrites on the wrapped expression.

This function is useful for testing and debugging query execution behavior. For example, you can use it to verify that a query returns correct results without relying on partition pruning.

## Syntax

```Haskell
materialize(x);
```

## Parameters

`x`: the expression to pass through. Supported data types are BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, VARCHAR, DATE, DATETIME, DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256, JSON, and VARBINARY.

## Return value

Returns the same value and type as the input.

## Examples

Prevent partition pruning on a date column:

```Plain Text
-- Without materialize: partition pruning applies
SELECT * FROM sales WHERE dt >= '2024-01-01';

-- With materialize: partition pruning is disabled on dt
SELECT * FROM sales WHERE materialize(dt) >= '2024-01-01';
```

Prevent constant folding:

```Plain Text
SELECT materialize(1 + 2);
+-------------------+
| materialize(1 + 2)|
+-------------------+
|                 3 |
+-------------------+
```

## Keywords

MATERIALIZE, materialize
