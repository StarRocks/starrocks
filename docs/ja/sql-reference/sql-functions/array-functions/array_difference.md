---
displayed_sidebar: docs
description: "ARRAY_DIFFERENCE関数は配列内の隣接する要素間の差を計算します。"
---

# array_difference

配列の各要素からその次の要素を引くことで、配列内の隣接する要素間の差を計算し、その差を含む配列を返します。

## Syntax

```SQL
array_difference(input)
```

## Parameters

`input`: 隣接する要素間の差を計算したい配列。

## Return value

入力配列と同じ長さの配列を返します。戻り値の要素型は、入力の要素型に応じて次のように決まります。

| 入力要素型 | 戻り値要素型 |
| --- | --- |
| `BOOLEAN`、`TINYINT`、`SMALLINT`、`INT`、または `BIGINT` | `BIGINT` |
| `LARGEINT` | `LARGEINT` |
| `FLOAT` または `DOUBLE` | `DOUBLE` |
| `DECIMAL(P, S)`（`P <= 38`） | `DECIMAL(min(P + 1, 38), S)` |

`ARRAY<DECIMAL256>`（`P > 38`）は現在サポートされていません。

## Examples

Example 1:

```Plain
mysql> SELECT array_difference([342, 32423, 213, 23432]);
+-----------------------------------------+
| array_difference([342,32423,213,23432]) |
+-----------------------------------------+
| [0,32081,-32210,23219]                  |
+-----------------------------------------+
```

Example 2:

```Plain
mysql> SELECT array_difference([342, 32423, 213, null, 23432]);
+----------------------------------------------+
| array_difference([342,32423,213,NULL,23432]) |
+----------------------------------------------+
| [0,32081,-32210,null,null]                   |
+----------------------------------------------+
```

Example 3:

```Plain
mysql> SELECT array_difference([1.2, 2.3, 3.2, 4324242.55]);
+--------------------------------------------+
| array_difference([1.2,2.3,3.2,4324242.55]) |
+--------------------------------------------+
| [0,1.1,0.9,4324239.35]                     |
+--------------------------------------------+
```

Example 4:

```Plain
mysql> SELECT array_difference([false, true, false]);
+----------------------------------------+
| array_difference([FALSE, TRUE, FALSE]) |
+----------------------------------------+
| [0,1,-1]                               |
+----------------------------------------+
```