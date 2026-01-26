---
displayed_sidebar: docs
---

# BIGINT

## 説明

BIGINT

8 バイトの符号付き整数です。値の範囲は [-9223372036854775808, 9223372036854775807] です。

## 例

`BIGINT`列を持つテーブルを作成し、最大制限に近い値を挿入します。

```sql
CREATE TABLE bigIntDemo (
    pk BIGINT(20) NOT NULL COMMENT "Range: -2^63 to 2^63-1"
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 1;

INSERT INTO bigIntDemo VALUES (123456789012345), (9223372036854775807);
```

```Plaintext
MySQL > SELECT * FROM bigIntDemo;
+---------------------+
| pk                  |
+---------------------+
|     123456789012345 |
| 9223372036854775807 |
+---------------------+
```