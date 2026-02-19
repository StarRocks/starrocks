---
displayed_sidebar: docs
---

# SMALLINT

## 説明

SMALLINT

2 バイトの符号付き整数です。値の範囲は [-32768, 32767] です。

## 例

`SMALLINT`列を持つテーブルを作成します。

```sql
CREATE TABLE smallintDemo (
    pk SMALLINT COMMENT "range [-32768, 32767]"
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 1;

INSERT INTO smallintDemo VALUES (32767);
```

```Plaintext
MySQL > SELECT * FROM smallintDemo;
+-------+
| pk    |
+-------+
| 32767 |
+-------+
```