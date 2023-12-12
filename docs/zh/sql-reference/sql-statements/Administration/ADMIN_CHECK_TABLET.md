---
displayed_sidebar: "Chinese"
---

# ADMIN CHECK TABLET

## 功能

该语句用于对一组 tablet 执行指定的检查操作。

:::tip

该操作需要 SYSTEM 级 OPERATE 权限。请参考 [GRANT](../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```sql
ADMIN CHECK TABLET (tablet_id1, tablet_id2, ...)
PROPERTIES("type" = "...")
```

说明：

1. 必须指定 tablet id 列表以及 PROPERTIES 中的 type 属性。
2. 目前 type 仅支持：

     **consistency**: 对 tablet 的副本数据一致性进行检查。该命令为异步命令，发送后，StarRocks 会开始执行对应 tablet 的一致性检查作业。最终的结果，将体现在 `SHOW PROC "/statistic";` 结果中的 InconsistentTabletNum 列。

## 示例

1. 对指定的一组 tablet 进行副本数据一致性检查

    ```sql
    ADMIN CHECK TABLET (10000, 10001)
    PROPERTIES("type" = "consistency");
    ```
