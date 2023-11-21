---
displayed_sidebar: "English"
---

# ifs

## Description

Function `ifs` is a more simple way to express `case when`, it's equivalent to the following sql

```SQL
CASE WHEN condition1 THEN result1
    [WHEN condition2 THEN result2]
    ...
    [WHEN conditionN THEN resultN]
    [ELSE result]
END
```

## Syntax

```Haskell
ifs(condition1,result1[,conditio2,result2]...[,result]);
```

## Parameters

`condition1~N`: should be compatible with bool type.

`result1~N` and `result` are compatible in data type.

## Return value

The return value is of the common type of all types from `result`.

## Examples

```Plain Text
mysql> select ifs(true, 1, false, 2, 0);
+---------------------------+
| ifs(TRUE, 1, FALSE, 2, 0) |
+---------------------------+
|                         1 |
+---------------------------+
1 row in set (0.01 sec)
```