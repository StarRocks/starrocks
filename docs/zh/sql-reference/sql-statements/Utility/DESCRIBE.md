# DESCRIBE

## 描述

该语句用于展示指定 table 的 schema 信息。

<<<<<<< HEAD
语法：
=======
- 查看 StarRocks 表结构、[排序键](../../../table_design/Sort_key.md) (Sort Key) 类型和[物化视图](../../../using_starrocks/Materialized_view.md)。
- 查看外部数据源（如 Apache Hive™）中的表结构。仅 StarRocks 2.4 及以上版本支持该操作。
>>>>>>> b8eb50e58 ([Doc] link fixes to 2.5 (#35185))

```sql
DESC[RIBE] [db_name.]table_name [ALL];
```

说明：

"[]" 中的内容可省略。如果指定 ALL，则显示该 table 的所有字段，索引及物化视图信息。

## 示例

1. 查看表字段信息  

    ```sql
    DESC table_name;
    ```

2. 查看表的字段，索引及物化视图信息

    ```sql
    DESC db1.table_name ALL;
    ```
