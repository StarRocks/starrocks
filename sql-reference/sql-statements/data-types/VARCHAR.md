# VARCHAR

## 描述

VARCHAR(M)

变长字符串，M 代表的是变长字符串的长度。M 的范围是 1~1048576，默认取值 1。自 2.1 版本开始，M 的范围为 1~1048576；2.1 之前的版本的 M 范围为 1~65533。

## 示例

创建表时指定字段类型为 VARCHAR。

```sql
CREATE TABLE varcharDemo (
    pk INT COMMENT "range [-2147483648, 2147483647]",
    pd_type VARCHAR(20) COMMENT "range char(m),m in (1-65533) "
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk) BUCKETS 4;
```
