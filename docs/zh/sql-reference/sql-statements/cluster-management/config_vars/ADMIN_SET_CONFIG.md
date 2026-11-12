---
displayed_sidebar: docs
description: "ADMIN SET CONFIG 为 StarRocks 集群在运行时设置 FE 动态配置项。"
products: [oss, enterprise, cloud, anywhere]
---

# ADMIN SET CONFIG

## 功能

该语句用于设置集群的配置项（当前仅支持设置 FE 的配置项）。

可设置的配置项，可以通过 [ADMIN SHOW FRONTEND CONFIG](ADMIN_SHOW_CONFIG.md) 命令查看。

设置后的配置项，在 FE 重启之后会恢复成 **fe.conf** 文件中的配置或者默认值。如果需要让配置长期生效，建议设置完之后同时修改 **fe.conf** 文件，防止重启后修改失效。自 v3.5.0 起，您可以在语句中添加 `WITH PERSISTENT`，将新的配置值同时写入各个 FE 的 **fe.conf** 文件，使其在 FE 重启后仍然生效。如果 **fe.conf** 不可写或 FE 运行在容器中，带 `WITH PERSISTENT` 的语句会执行失败。

:::tip

该操作需要 SYSTEM 级 OPERATE 权限。请参考 [GRANT](../../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```sql
ADMIN SET FRONTEND CONFIG ("key" = "value") [WITH PERSISTENT]
```

## 示例

设置 `tablet_sched_disable_balance` 为 `true`。

```sql
 ADMIN SET FRONTEND CONFIG ("tablet_sched_disable_balance" = "true");
```
