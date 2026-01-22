---
displayed_sidebar: docs
---

# FLOAT

## 説明

FLOAT

4 バイトの浮動小数点数

## 例

`FLOAT`列（4バイト）を持つテーブルを作成します。`FLOAT`の精度は約7桁の有効数字です。

```sql
CREATE TABLE floatDemo (
    pk BIGINT(20) NOT NULL,
    channel FLOAT COMMENT "4 bytes"
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 1;

INSERT INTO floatDemo VALUES (1, 12345.67890123456789);
```

```Plaintext
MySQL > SELECT * FROM floatDemo;
+------+-----------+
| pk   | channel   |
+------+-----------+
|    1 | 12345.679 |
+------+-----------+
```