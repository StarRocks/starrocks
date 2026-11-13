---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "数値の常用対数を計算します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# log10, dlog10

数値の常用対数を計算します。

## Syntax

```SQL
log10(arg)
```

## Parameters

- `arg`: 対数を計算したい値。DOUBLE データ型のみサポートされています。

> **NOTE**
>
> StarRocks は、`arg` が負または 0 の場合、NULL を返します。

## Return value

DOUBLE データ型の値を返します。

## Example

例 1: 100 の常用対数を計算します。

```Plain
select log10(100);
+------------+
| log10(100) |
+------------+
|          2 |
+------------+
1 row in set (0.02 sec)
```