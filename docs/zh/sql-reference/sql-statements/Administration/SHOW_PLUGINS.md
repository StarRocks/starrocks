---
displayed_sidebar: "Chinese"
---

# SHOW PLUGINS

## 功能

该语句用于展示已安装的插件。

:::tip

该操作需要 SYSTEM 级 PLUGIN 权限。请参考 [GRANT](../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```sql
SHOW PLUGINS
```

该命令会展示所有用户安装的和系统内置的插件。

## 示例

1. 展示已安装的插件：

    ```sql
    SHOW PLUGINS;
    ```
