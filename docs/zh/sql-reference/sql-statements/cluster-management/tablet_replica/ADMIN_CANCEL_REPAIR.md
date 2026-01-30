---
displayed_sidebar: docs
---

# ADMIN CANCEL REPAIR

## 功能

取消对指定表或分区执行优先级修复操作的计划。该语句仅表示系统不再以高优先级修复指定表或分区的分片副本。系统仍会以默认调度方式修复副本。

ADMIN CANCEL REPAIR 仅适用于存算一体集群中的内表。

详细操作指南请参阅[管理副本](../../../../administration/management/resource_management/Replica.md)。

:::tip

该操作需要 SYSTEM 级 OPERATE 权限。请参考 [GRANT](../../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```sql
ADMIN CANCEL REPAIR TABLE table_name[ PARTITION (p1,...)]
```

## 示例

1. 取消内表 `tbl1` 中 `p1` 分区的高优先级修复计划。

   ```sql
   ADMIN CANCEL REPAIR TABLE tbl PARTITION(p1);
   ```
