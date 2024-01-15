---
displayed_sidebar: "Chinese"
---

# ADMIN SHOW REPLICA DISTRIBUTION

## 功能

该语句用于展示一个表或分区副本分布状态。

:::tip

该操作需要 SYSTEM 级 OPERATE 权限。请参考 [GRANT](../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```sql
ADMIN SHOW REPLICA DISTRIBUTION FROM [db_name.]tbl_name [PARTITION (p1, ...)]
```

说明：

结果中的 Graph 列以图形的形式展示副本分布比例。

## 示例

1. 查看表的副本分布

    ```sql
    ADMIN SHOW REPLICA DISTRIBUTION FROM tbl1;
    ```

2. 查看表的分区的副本分布

    ```sql
    ADMIN SHOW REPLICA DISTRIBUTION FROM db1.tbl1 PARTITION(p1, p2);
    ```
