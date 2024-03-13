---
displayed_sidebar: "English"
---

# rand, random

## Description

Returns a random floating-point number between 0 (inclusive) and 1 (exclusive).

## Syntax

```Haskell
RAND(x);
```

## Parameters

`x`: optional. The data type is BIGINT. If `x` is specified, this function returns a repeatable random number. If `x` is not specified, this function returns a completely random number.

## Return value

Returns a value of the DOUBLE type.

## Examples

```Plain Text
select rand();
+--------------------+
| rand()             |
+--------------------+
| 0.9393535880089522 |
+--------------------+
1 row in set (0.01 sec)
select rand(3);
+--------------------+
| rand(3)            |
+--------------------+
| 0.6659865964511347 |
+--------------------+
1 row in set (0.00 sec)
```

## Keywords

RAND, RANDOM
