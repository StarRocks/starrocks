---
displayed_sidebar: docs
---

# SHOW PLUGINS

## 説明

このステートメントは、インストールされているプラグインを表示するために使用します。

:::tip

この操作には、SYSTEM レベルの PLUGIN 権限が必要です。この権限を付与するには、[GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

## 構文

```sql
SHOW PLUGINS
```

このコマンドは、すべての組み込みプラグインとカスタムプラグインを表示します。

## 例

1. インストールされているプラグインを表示する:

    ```sql
    SHOW PLUGINS;
    ```