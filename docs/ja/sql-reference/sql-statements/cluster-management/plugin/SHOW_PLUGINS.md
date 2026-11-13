---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "SHOW PLUGINS は、インストールされているプラグインを表示するために使用します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
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