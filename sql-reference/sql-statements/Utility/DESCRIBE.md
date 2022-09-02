# DESC

## 功能

查看 StarRocks 中的表结构或外部数据源中的表结构，例如 Apache Hive™, Apache Iceberg, 和 Apache Hudi。仅 StarRocks 2.4 及以上版本支持查看外部数据的表结构。

## 语法

```sql
DESC[RIBE] [db_name.]table_name [ALL];
```

说明：

如果指定 ALL，则显示该 table 的所有字段，索引及物化视图信息。

## 示例

示例一：查看表字段信息。

 ```sql
DESC table_name;
```

示例二：查看表的字段，索引及物化视图信息。

```sql
DESC db1.table_name ALL;
```
