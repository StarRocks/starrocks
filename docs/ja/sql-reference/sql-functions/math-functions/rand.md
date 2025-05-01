---
displayed_sidebar: docs
---

# rand, random

## Description

0（含む）から1（含まない）の間のランダムな浮動小数点数を返します。

## Syntax

```Haskell
RAND(x);
```

## Parameters

`x`: オプションです。データ型は BIGINT です。`x` が指定されている場合、この関数は再現可能なランダム数を返します。`x` が指定されていない場合、この関数は完全にランダムな数を返します。

## Return value

DOUBLE 型の値を返します。

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