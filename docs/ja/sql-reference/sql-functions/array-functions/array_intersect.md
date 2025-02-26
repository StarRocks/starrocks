---
displayed_sidebar: docs
---

# array_intersect

1 つ以上の配列の共通部分にある要素の配列を返します。

## Syntax

```Haskell
array_intersect(input0, input1, ...)
```

## Parameters

`input`: 共通部分を取得したい 1 つ以上の配列。`(input0, input1, ...)` の形式で配列を指定し、指定する配列が同じデータ型であることを確認してください。

## Return value

指定した配列と同じデータ型の配列を返します。

## Examples

Example 1:

```Plain
mysql> SELECT array_intersect(["SQL", "storage"], ["mysql", "query", "SQL"], ["SQL"])
AS no_intersect ;
+--------------+
| no_intersect |
+--------------+
| ["SQL"]      |
+--------------+
```

Example 2:

```Plain
mysql> SELECT array_intersect(["SQL", "storage"], ["mysql", null], [null]) AS no_intersect ;
+--------------+
| no_intersect |
+--------------+
| []           |
+--------------+
```

Example 3:

```Plain
mysql> SELECT array_intersect(["SQL", null, "storage"], ["mysql", null], [null]) AS no_intersect ;
+--------------+
| no_intersect |
+--------------+
| [null]       |
+--------------+
```