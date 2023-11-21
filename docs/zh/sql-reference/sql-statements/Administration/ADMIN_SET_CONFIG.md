---
displayed_sidebar: "Chinese"
---

# ADMIN SET CONFIG

## 功能

该语句用于设置集群的配置项（当前仅支持设置 FE 的配置项）。

可设置的配置项，可以通过 [ADMIN SHOW FRONTEND CONFIG](ADMIN_SHOW_CONFIG.md) 命令查看。

设置后的配置项，在 FE 重启之后会恢复成 **fe.conf** 文件中的配置或者默认值。如果需要让配置长期生效，建议设置完之后同时修改 **fe.conf** 文件，防止重启后修改失效。

## 语法

```sql
ADMIN SET FRONTEND CONFIG ("key" = "value")
```

## 示例

设置 `tablet_sched_disable_balance` 为 `true`。

```sql
 ADMIN SET FRONTEND CONFIG ("tablet_sched_disable_balance" = "true");
```
