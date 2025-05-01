---
displayed_sidebar: docs
---

# UNINSTALL PLUGIN

## 説明

このステートメントはプラグインをアンインストールするために使用されます。

:::tip

この操作には SYSTEM レベルの PLUGIN 権限が必要です。 [GRANT](../../account-management/GRANT.md) の指示に従って、この権限を付与することができます。

:::

## 構文

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