---
displayed_sidebar: "Chinese"
---

# UNINTALL PLUGIN

## description

该语句用于卸载一个插件。

语法

```SQL
UNINSTALL PLUGIN plugin_name;
```

plugin_name 可以通过 `SHOW PLUGINS;` 命令查看。

只能卸载非 builtin 的插件。

## example

1. 卸载一个插件：

    ```SQL
    UNINSTALL PLUGIN auditdemo;
    ```

## keyword

UNINSTALL,PLUGIN
