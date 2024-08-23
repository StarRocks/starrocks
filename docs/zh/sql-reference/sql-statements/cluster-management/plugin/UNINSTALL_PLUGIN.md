---
displayed_sidebar: docs
---

# UNINSTALL PLUGIN

## 功能

该语句用于卸载一个插件。

:::tip

该操作需要 SYSTEM 级 PLUGIN 权限。请参考 [GRANT](../../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```SQL
UNINSTALL PLUGIN plugin_name
```

plugin_name 可以通过 `SHOW PLUGINS;` 命令查看。

只能卸载非 builtin 的插件。

## 示例

1. 卸载一个插件：

    ```SQL
    UNINSTALL PLUGIN auditdemo;
    ```
