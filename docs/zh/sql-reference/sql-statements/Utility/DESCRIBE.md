# DESCRIBE

## 描述

该语句用于展示指定 table 的 schema 信息。

语法：

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
