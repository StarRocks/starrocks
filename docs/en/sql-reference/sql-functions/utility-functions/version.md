---
displayed_sidebar: "English"
---

# version

## Description

Returns the current version of the MySQL database.

You can use [current_version](current_version.md) to query StarRocks version.

## Syntax

```Haskell
VARCHAR version();
```

## Parameters

None

## Return value

Returns a value of the VARCHAR type.

## Examples

```Plain Text
mysql> select version();
+-----------+
| version() |
+-----------+
| 5.1.0     |
+-----------+
1 row in set (0.00 sec)
```

## References

[current_version](../utility-functions/current_version.md)
