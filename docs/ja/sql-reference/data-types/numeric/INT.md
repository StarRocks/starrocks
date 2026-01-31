---
displayed_sidebar: docs
---

# INT

## 説明

INT

4 バイトの符号付き整数です。値の範囲は [-2147483648, 2147483647] です。

## 例

`INT`列を持つテーブルを作成し、許容される最小値と最大値を挿入します。

```sql
CREATE TABLE intDemo (
    pk INT COMMENT "range [-2147483648, 2147483647]"
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 1;

INSERT INTO intDemo VALUES (2147483647), (-2147483648);
```

```Plaintext
MySQL > SELECT * FROM intDemo;
+-------------+
| pk          |
+-------------+
| -2147483648 |
|  2147483647 |
+-------------+
```