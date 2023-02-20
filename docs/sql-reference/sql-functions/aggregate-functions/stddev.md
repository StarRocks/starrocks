
# STDDEV,STDDEV_POP

## Description

Returns the population standard deviation of the expr expression.

## Syntax

```Haskell
STDDEV(expr)
```

## Examples

```plain text
mysql> SELECT  stddev(lo_quantity), stddev_pop(lo_quantity) from lineorder;
+---------------------+-------------------------+
| stddev(lo_quantity) | stddev_pop(lo_quantity) |
+---------------------+-------------------------+
|   14.43100708360797 |       14.43100708360797 |
+---------------------+-------------------------+
```

## keyword

STDDEV,STDDEV_POP,POP
