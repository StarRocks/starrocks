---
displayed_sidebar: docs
---

# DOUBLE

## 説明

DOUBLE

8 バイト浮動小数点数

## 例

高精度の小数を扱うために`DOUBLE`列（8バイト）を持つテーブルを作成します。

```sql
CREATE TABLE doubleDemo (
    pk BIGINT(20) NOT NULL,
    income DOUBLE COMMENT "8 bytes, high precision"
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 1;

INSERT INTO doubleDemo VALUES (1, 12345.67890123456789);
```

```Plaintext
MySQL > SELECT * FROM doubleDemo;
+------+--------------------+
| pk   | income             |
+------+--------------------+
|    1 | 12345.678901234567 |
+------+--------------------+
```