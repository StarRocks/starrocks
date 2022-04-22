# ALTER VIEW

## 功能

该语句用于修改一个view的定义。

## 语法

```sql
ALTER VIEW
[db_name.]view_name
(column1[ COMMENT "col comment"][, column2, ...])
AS query_stmt;
```

注：方括号 [] 中内容可省略不写。

说明：

1. 视图都是逻辑上的，其中的数据不会存储在物理介质上，在查询时视图将作为语句中的子查询，因此，修改视图的定义等价于修改query_stmt。
2. query_stmt 为任意支持的 SQL。

## example

1. 修改example_db上的视图example_view。

    ```sql
    ALTER VIEW example_db.example_view
    (
        c1 COMMENT "column 1",
        c2 COMMENT "column 2",
        c3 COMMENT "column 3"
    )
    AS SELECT k1, k2, SUM(v1) 
    FROM example_table
    GROUP BY k1, k2;
    ```
