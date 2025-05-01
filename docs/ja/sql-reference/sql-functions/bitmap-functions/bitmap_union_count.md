---
displayed_sidebar: docs
---

# bitmap_union_count

## Description

ビットマップ値の集合の合併を返し、その合併の基数を返します。この関数は v2.3 からサポートされています。

## Syntax

```Haskell
BIGINT bitmap_union_count(BITMAP value)
```

### Parameters

`value`: ビットマップ値の集合。サポートされているデータ型は BITMAP です。

## Return value

BIGINT 型の値を返します。

## Examples

ウェブページのユニークビュー (UV) を計算します。`user_id` が INT 型の場合、後者の2つのクエリは同等です。

```Plaintext
mysql> select * from test
+---------+---------+
| page_id | user_id |
+---------+---------+
|       1 |       1 |
|       1 |       2 |
|       2 |       1 |
+---------+---------+

mysql> select page_id,count(distinct user_id) from test group by page_id;
+---------+-------------------------+
| page_id | count(DISTINCT user_id) |
+---------+-------------------------+
|       1 |                       2 |
|       2 |                       1 |
+---------+-------------------------+

mysql> select page_id,bitmap_union_count(to_bitmap(user_id)) from test group by page_id;
+---------+----------------------------------------+
| page_id | bitmap_union_count(to_bitmap(user_id)) |
+---------+----------------------------------------+
|       1 |                                      2 |
|       2 |                                      1 |
+---------+----------------------------------------+
```