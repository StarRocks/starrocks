---
keywords: ['xiugai', 'shitu'] 
displayed_sidebar: docs
description: "ALTER VIEW modifies the definition of a view."
---

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

```sql
ALTER VIEW [db_name.]view_name RENAME new_view_name
```

说明：

1. 逻辑视图中的数据不会存储在物理介质上，在查询时，逻辑视图将作为语句中的子查询，因此，修改逻辑视图的定义等价于修改 query_stmt。
2. query_stmt 为任意支持的 SQL。
3. RENAME 仅修改逻辑视图的名称。该视图上已授予的权限、注释以及 SQL SECURITY 特性均保持不变。

:::warning
逻辑视图是按名称被引用的。重命名之后，仍然使用旧名称的其他逻辑视图、物化视图或应用 SQL 会在查询时报“表不存在”的错误，重命名时不会有任何提示。基于该视图创建的物化视图会被置为 inactive，且在您将视图改回原名或重建这些物化视图之前无法重新激活。
:::

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

2. 将 example_db 上的逻辑视图 example_view 重命名为 example_view_new_name。

    ```sql
    ALTER VIEW example_db.example_view RENAME example_view_new_name;
    ```
