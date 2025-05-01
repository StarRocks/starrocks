---
displayed_sidebar: docs
---

# any_value

## 説明

各集約グループから任意の行を取得します。この関数を使用して、`GROUP BY`句を含むクエリを最適化できます。

## 構文

```Haskell
ANY_VALUE(expr)
```

## パラメータ

`expr`: 集約される式です。v3.2以降、`expr`はARRAY、MAP、およびSTRUCTに評価できます。

## 戻り値

各集約グループから任意の行を返します。戻り値は非決定的です。

## 例

テーブル`t0`を作成し、このテーブルにデータを挿入します。

```sql
CREATE TABLE t0(
  a INT,
  b BIGINT,
  c SMALLINT,
  d ARRAY<INT>,
  e JSON
)
DUPLICATE KEY(a)
DISTRIBUTED BY HASH(a);

INSERT INTO t0 VALUES
(1, 1, 1, [2,3,4],parse_json('{"a":1, "b":true}')),
(1, 2, 1, [2,3,5],parse_json('{"a":2, "b":true}')),
(2, 1, 1, [2,3,6],parse_json('{"a":3, "b":true}')),
(2, 2, 2, [2,4,5],parse_json('{"a":4, "b":false}')),
(3, 1, 1, [3,3,5],parse_json('{"a":5, "b":false}'));
```

```plain text
mysql> select * from t0 order by a;
+------+------+------+---------+----------------------+
| a    | b    | c    | d       | e                    |
+------+------+------+---------+----------------------+
|    1 |    1 |    1 | [2,3,4] | {"a": 1, "b": true}  | 
|    1 |    2 |    1 | [2,3,5] | {"a": 2, "b": true}  | 
|    2 |    1 |    1 | [2,3,6] | {"a": 3, "b": true}  | 
|    2 |    2 |    2 | [2,4,5] | {"a": 4, "b": false} | 
|    3 |    1 |    1 | [3,3,5] | {"a": 5, "b": false} | 
+------+------+------+---------+----------------------+
5 rows in set (0.01 sec)
```

`any_value`を使用してデータをクエリします。`a = 1`および`a = 2`の場合、`b`に対して任意の行が返されます。

```plain text
mysql> select a,any_value(b),sum(c) from t0 group by a;
+------+----------------+----------+
| a    | any_value(`b`) | sum(`c`) |
+------+----------------+----------+
|    1 |              1 |        2 |
|    2 |              1 |        3 |
|    3 |              1 |        1 |
+------+----------------+----------+
3 rows in set (0.01 sec)
```