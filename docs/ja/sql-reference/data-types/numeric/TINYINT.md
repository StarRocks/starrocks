---
displayed_sidebar: docs
---

# TINYINT

## 説明

TINYINT

1 バイトの符号付き整数です。値の範囲は [-128, 127] です。

## 例

`TINYINT`列を持つテーブルを作成します。

```sql
CREATE TABLE tinyIntDemo (
    pk TINYINT COMMENT "range [-128, 127]",
    pd_type VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 1;

INSERT INTO tinyIntDemo VALUES (127, 'Max Value');
```

```Plaintext
MySQL > SELECT * FROM tinyIntDemo;
+------+-----------+
| pk   | pd_type   |
+------+-----------+
|  127 | Max Value |
+------+-----------+
```