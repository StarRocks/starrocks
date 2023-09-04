
# STDDEV,STDDEV_POP,std

## Description

Returns the population standard deviation of the expr expression. Since v2.5.10, this function can also be used as a window function.

## Syntax

```Haskell
STDDEV(expr)
```

## Parameters

`expr`: the expression. If it is a table column, it must evaluate to TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

## Return value

Returns a DOUBLE value.

## Examples

```plaintext
mysql> SELECT stddev(lo_quantity), stddev_pop(lo_quantity) from lineorder;
+---------------------+-------------------------+
| stddev(lo_quantity) | stddev_pop(lo_quantity) |
+---------------------+-------------------------+
|   14.43100708360797 |       14.43100708360797 |
+---------------------+-------------------------+
```

## keyword

STDDEV,STDDEV_POP,POP
