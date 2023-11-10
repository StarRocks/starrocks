---
displayed_sidebar: "English"
---

# uuid_numeric

## Description

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
