---
displayed_sidebar: docs
sidebar_position: 25
sidebar_label: "ビルトインロール"
---

import DBAdmin from '../../../_assets/commonMarkdown/role_db_admin.mdx'
import ClusterAdmin from '../../../_assets/commonMarkdown/role_cluster_admin.mdx'

# StarRocks がサポートするビルトインロール

StarRocks クラスターには、5 つのビルトインロールがあります。

- `db_admin`
- `cluster_admin`
- `user_admin`
- `security_admin`
- `public`

各 `admin` ロールには、特定のドメインで管理操作を行うための異なる権限が付与されています。デフォルトでは、`public` ロールには権限がなく、クラスターにアクセスできるすべてのユーザーに付与されます。

以下に説明する権限の詳細については、 [Privilege Item](./privilege_item.md) を参照してください。

<DBAdmin />

<ClusterAdmin />

## `user_admin`

`user_admin` はビルトインのユーザー管理者です。ユーザー、ロール、および認可を管理するために使用できます。

- ユーザーと権限の管理に特化
- ユーザーの作成、変更、削除が可能
- 権限やロールの付与または取り消しが可能
- 不変のロール

権限の範囲:

| Privilege Level   | Privilege Item |
| ----------------- | -------------- |
| SYSTEM            | GRANT          |

## `security_admin`

`security_admin` はビルトインのセキュリティ管理者です。セキュリティ統合とグループプロバイダーを管理するために使用できます。

- システムセキュリティの管理に特化
- セキュリティ関連の設定や戦略を管理可能
- 不変のロール

権限の範囲:

| Privilege Level   | Privilege Item |
| ----------------- | -------------- |
| SYSTEM            | <ul><li>SECURITY</li><li>OPERATE</li></ul> |

## `public`

`public` はクラスターにアクセスできるすべてのユーザーに付与されるビルトインロールです。デフォルトでは、権限はありません。

- すべてのクラスター ユーザーに自動的に付与およびアクティブ化される
- 変更可能なロール。すべてのクラスター ユーザーに権限やロールを付与したい場合、このロールに付与できます。