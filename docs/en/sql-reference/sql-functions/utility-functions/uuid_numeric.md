---
displayed_sidebar: docs
---

# uuid_numeric

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

Returns a random UUID of the LARGEINT type. This function has an execution performance 2 orders of magnitude better than the `uuid` function.

## Syntax

```Haskell
uuid_numeric();
```

## Parameters

None

## Return value

Returns a value of the LARGEINT type.

## Examples

```Plain Text
MySQL > select uuid_numeric();
+--------------------------+
| uuid_numeric()           |
+--------------------------+
| 558712445286367898661205 |
+--------------------------+
1 row in set (0.00 sec)
```
