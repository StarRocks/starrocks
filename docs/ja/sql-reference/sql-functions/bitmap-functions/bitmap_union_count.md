---
displayed_sidebar: docs
---

# bitmap_union_count

一連のビットマップ値の和集合を返し、その和集合の基数を返します。この関数は v2.3 からサポートされています。

## 構文

```Haskell
BIGINT bitmap_union_count(BITMAP value)
```

### パラメータ

`value`: ビットマップ値の集合。サポートされているデータ型は BITMAP です。

## 戻り値

BIGINT 型の値を返します。

## 例

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