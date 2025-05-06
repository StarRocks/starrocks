---
displayed_sidebar: docs
---

# array_intersect

## 説明

1つ以上の配列の共通部分にある要素を含む配列を返します。

## 構文

```Haskell
array_intersect(input0, input1, ...)
```

## パラメータ

`input`: 共通部分を取得したい1つ以上の配列を指定します。配列は `(input0, input1, ...)` の形式で指定し、指定する配列が同じデータ型であることを確認してください。

## 戻り値

指定した配列と同じデータ型の配列を返します。

## 例

例 1:

```Plain
mysql> SELECT array_intersect(["SQL", "storage"], ["mysql", "query", "SQL"], ["SQL"])
AS no_intersect ;
+--------------+
| no_intersect |
+--------------+
| ["SQL"]      |
+--------------+
```

例 2:

```Plain
mysql> SELECT array_intersect(["SQL", "storage"], ["mysql", null], [null]) AS no_intersect ;
+--------------+
| no_intersect |
+--------------+
| []           |
+--------------+
```

例 3:

```Plain
mysql> SELECT array_intersect(["SQL", null, "storage"], ["mysql", null], [null]) AS no_intersect ;
+--------------+
| no_intersect |
+--------------+
| [null]       |
+--------------+
```