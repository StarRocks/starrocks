---
displayed_sidebar: docs
---

# array_unique_agg

## 説明

ARRAY列の異なる値（`NULL`を含む）を集約して、配列にまとめます（複数行から1行に）。

この関数はv3.2からサポートされています。

## 構文

```Haskell
ARRAY_UNIQUE_AGG(col)
```

## パラメータ

- `col`: 集約したい列の値。サポートされているデータ型はARRAYです。

## 戻り値

ARRAY型の値を返します。

## 使用上の注意

- 配列内の要素の順序はランダムです。
- 返される配列内の要素のデータ型は、入力列の要素のデータ型と同じです。
- 一致する値がない場合は`NULL`を返します。

## 例

以下のデータテーブルを例にとります。

```plaintext
mysql > select * from array_unique_agg_example;
+------+--------------+
| a    | b            |
+------+--------------+
|    2 | [1,null,2,4] |
|    2 | [1,null,3]   |
|    1 | [1,1,2,3]    |
|    1 | [2,3,4]      |
+------+--------------+
```

例1: 列`a`の値をグループ化し、列`b`の異なる値を配列に集約します。

```plaintext
mysql > select a, array_unique_agg(b) from array_unique_agg_example group by a;
+------+---------------------+
| a    | array_unique_agg(b) |
+------+---------------------+
|    1 | [4,1,2,3]           |
|    2 | [4,1,2,3,null]      |
+------+---------------------+
```

例2: WHERE句を使用して列`b`の値を集約します。フィルター条件に合致するデータがない場合、`NULL`値が返されます。

```plaintext
mysql > select array_unique_agg(b) from array_unique_agg_example where a < 0;
+---------------------+
| array_unique_agg(b) |
+---------------------+
| NULL                |
+---------------------+
```

## キーワード

ARRAY_UNIQUE_AGG, ARRAY