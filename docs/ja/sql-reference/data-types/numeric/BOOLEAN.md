---
displayed_sidebar: docs
---

# BOOLEAN

## 説明

BOOL, BOOLEAN

TINYINT と同様に、0 は false を表し、1 は true を表します。

## 例

`BOOLEAN`列を持つテーブルを作成します。StarRocksは`TRUE`を`1`に、`FALSE`を`0`に変換することに注意してください。

```sql
CREATE TABLE booleanDemo (
    pk INT COMMENT "Primary Key",
    ispass BOOLEAN COMMENT "true/false"
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 1;

INSERT INTO booleanDemo VALUES (1, true), (2, false), (3, 1), (4, 0);
```

```Plaintext
MySQL > SELECT * FROM booleanDemo;
+------+--------+
| pk   | ispass |
+------+--------+
|    1 |      1 |
|    2 |      0 |
|    3 |      1 |
|    4 |      0 |
+------+--------+
```