---
displayed_sidebar: docs
---

# システムメタデータベース

このトピックでは、システム定義ビューを通じて StarRocks クラスターの専用メタデータを表示する方法について説明します。

各 StarRocks クラスターは、いくつかの読み取り専用のシステム定義ビューを含むデータベース `sys` を維持しています。これらのメタデータビューは、特権構造、オブジェクト依存関係、その他の情報に関する全体的な洞察を提供する統一された使いやすいインターフェースを提供します。

## `sys` を通じてメタデータ情報を表示する

StarRocks インスタンス内の専用メタデータ情報は、`sys` 内のビューの内容をクエリすることで表示できます。

次の例では、ビュー `grants_to_roles` をクエリして、ユーザー定義ロールに付与された特権を確認します。

```Plain
MySQL > SELECT * FROM sys.grants_to_roles LIMIT 5\G
*************************** 1. row ***************************
        GRANTEE: role_test
 OBJECT_CATALOG: default_catalog
OBJECT_DATABASE: db_test
    OBJECT_NAME: tbl1
    OBJECT_TYPE: TABLE
 PRIVILEGE_TYPE: SELECT, ALTER
   IS_GRANTABLE: NO
*************************** 2. row ***************************
        GRANTEE: role_test
 OBJECT_CATALOG: default_catalog
OBJECT_DATABASE: db_test
    OBJECT_NAME: tbl2
    OBJECT_TYPE: TABLE
 PRIVILEGE_TYPE: SELECT
   IS_GRANTABLE: YES
*************************** 3. row ***************************
        GRANTEE: role_test
 OBJECT_CATALOG: default_catalog
OBJECT_DATABASE: db_test
    OBJECT_NAME: mv_test
    OBJECT_TYPE: MATERIALIZED VIEW
 PRIVILEGE_TYPE: SELECT
   IS_GRANTABLE: YES
*************************** 4. row ***************************
        GRANTEE: role_test
 OBJECT_CATALOG: NULL
OBJECT_DATABASE: NULL
    OBJECT_NAME: NULL
    OBJECT_TYPE: SYSTEM
 PRIVILEGE_TYPE: CREATE RESOURCE GROUP
   IS_GRANTABLE: NO
```

## `sys` 内のビュー

`sys` には次のメタデータビューが含まれています:

| **View**             | **Description**                                                               |
| -------------------- | ----------------------------------------------------------------------------- |
| [grants_to_roles](../sys/grants_to_roles.md)         | ユーザー定義ロールに付与された特権の情報を記録します。 |
| [grants_to_users](../sys/grants_to_users.md)         | ユーザーに付与された特権の情報を記録します。              |
| [role_edges](../sys/role_edges.md)                   | ロールの被付与者を記録します。                            |
| [object_dependencies](../sys/object_dependencies.md) | 非同期マテリアライズドビューの依存関係を記録します。       |

:::note

アプリケーションのシナリオに応じて、`sys` 内のビューはデフォルトで一部の `admin` ロールにのみアクセス可能です。特定のニーズに応じて、これらのビューに対する SELECT 特権を他のユーザーに付与することができます。

:::