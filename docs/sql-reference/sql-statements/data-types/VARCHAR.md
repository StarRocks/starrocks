# VARCHAR

## Description

VARCHAR(M)

A variable-length string. `M` indicates the length of the string. The default value is `1`. Unit: bytes.

- In versions earlier than StarRocks 2.1, the value range of `M` is 1–65533.
- [Preview] In StarRocks 2.1 and later versions, the value range of `M` is 1–1048576.

## Examples

Create a table and specify the column type as VARCHAR.

```SQL
CREATE TABLE varcharDemo (
    pk INT COMMENT "range [-2147483648, 2147483647]",
    pd_type VARCHAR(20) COMMENT "range char(m),m in (1-65533) "
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk) BUCKETS 4;
```
