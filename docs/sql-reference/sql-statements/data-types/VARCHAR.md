# VARCHAR

## Description

VARCHAR(M)

A variable-length string. `M` represents the length of a variable-length string. The range of `M` is 1–1048576. The default value is 1. In versions earlier than 2.1, the range of `M` is 1–65533. From 2.1 onwards, the range of `M` is 1–1048576.

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
