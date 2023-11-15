# ALTER VIEW

## 功能

该语句用于修改一个逻辑视图的定义。

## 语法

```sql
ALTER VIEW
[db_name.]view_name
(column1[ COMMENT "col comment"][, column2, ...])
AS query_stmt
```

说明：

1. 逻辑视图中的数据不会存储在物理介质上，在查询时，逻辑视图将作为语句中的子查询，因此，修改逻辑视图的定义等价于修改 query_stmt。
2. query_stmt 为任意支持的 SQL。

## 示例

1. 修改example_db上的逻辑视图example_view。

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
