# DATE

## 描述

日期类型，目前的取值范围是 ['0000-01-01', '9999-12-31']，默认的打印形式是 'YYYY-MM-DD'。

## 示例

创建表时指定字段类型为 DATE。

```sql
CREATE TABLE dateDemo (
    pk INT COMMENT "range [-2147483648, 2147483647]",
    make_time DATE NOT NULL COMMENT "YYYY-MM-DD"
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk);
```
