---
displayed_sidebar: docs
---

# LARGEINT

## 説明

LARGEINT

16 バイトの符号付き整数です。値の範囲は [-2^127 + 1, 2^127 - 1] です。

## 例

`BIGINT`の範囲を超える数値を格納するために`LARGEINT`列（128ビット）を持つテーブルを作成します。

```sql
CREATE TABLE largeIntDemo (
    pk LARGEINT COMMENT "range [-2^127 + 1 ~ 2^127 - 1]"
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 1;

-- BIGINTの最大値（約900京）を超える値を挿入
INSERT INTO largeIntDemo VALUES (10000000000000000000000000);
```

```Plaintext
MySQL > SELECT * FROM largeIntDemo;
+----------------------------+
| pk                         |
+----------------------------+
| 10000000000000000000000000 |
+----------------------------+
```