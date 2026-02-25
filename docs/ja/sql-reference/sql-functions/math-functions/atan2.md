---
displayed_sidebar: docs
---

# atan2

`x` を `y` で割ったアークタンジェント、つまり `x/y` のアークタンジェントを返します。2 つのパラメータの符号は、結果の象限を決定するために使用されます。

戻り値は [-π, π] の範囲にあります。

## Syntax

```Haskell
ATAN2(x,y);
```

## Parameters

`x`: サポートされているデータ型は DOUBLE です。

`y`: サポートされているデータ型は DOUBLE です。

## Return value

DOUBLE 型の値を返します。`x` または `y` が NULL の場合は NULL を返します。

## Examples

```Plain Text
mysql> select atan2(-0.8,2);
+---------------------+
| atan2(-0.8, 2)      |
+---------------------+
| -0.3805063771123649 |
+---------------------+
1 row in set (0.01 sec)
```