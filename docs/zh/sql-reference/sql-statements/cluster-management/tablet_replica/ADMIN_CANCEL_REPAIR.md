---
displayed_sidebar: docs
---

# ADMIN CANCEL REPAIR

## 功能

<<<<<<< HEAD
该语句用于取消以高优先级修复指定表或分区。
=======
取消对指定表或分区执行优先级修复操作的计划。该语句仅表示系统不再以高优先级修复指定表或分区的分片副本。系统仍会以默认调度方式修复副本。

ADMIN CANCEL REPAIR 仅适用于存算一体集群中的内表。
>>>>>>> 9d6586f1d4 ([Doc] Remove Problematic Links (#69829))

:::tip

该操作需要 SYSTEM 级 OPERATE 权限。请参考 [GRANT](../../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```sql
ADMIN CANCEL REPAIR TABLE table_name[ PARTITION (p1,...)]
```

说明：该语句仅表示系统不再以高优先级修复指定表或分区的分片副本。系统仍会以默认调度方式修复副本。

## 示例

取消高优先级修复。

```sql
ADMIN CANCEL REPAIR TABLE tbl PARTITION(p1);
```
