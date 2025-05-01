---
displayed_sidebar: docs
---

# UNINSTALL PLUGIN

## 説明

このステートメントはプラグインをアンインストールするために使用されます。

構文:

```SQL
UNINSTALL PLUGIN <plugin_name>
```

plugin_name は SHOW PLUGINS コマンドで確認できます。

組み込みでないプラグインのみアンインストール可能です。

## 例

1. プラグインをアンインストールする:

    ```SQL
    UNINSTALL PLUGIN auditdemo;
    ```