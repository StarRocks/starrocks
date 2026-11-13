---
displayed_sidebar: docs
description: "StarRocks の 5 つのビルトインロール（db_admin, cluster_admin, user_admin, security_admin, public）とその権限説明。"
sidebar_position: 25
sidebar_label: "ビルトインロール"
products: [oss, enterprise, cloud, anywhere]
---


# StarRocks がサポートするビルトインロール

StarRocks クラスターには、5 つのビルトインロールがあります。

- `db_admin`
- `cluster_admin`
- `user_admin`
- `security_admin`
- `public`

各 `admin` ロールには、特定のドメインで管理操作を行うための異なる権限が付与されています。デフォルトでは、`public` ロールには権限がなく、クラスターにアクセスできるすべてのユーザーに付与されます。

以下に説明する権限の詳細については、 [Privilege Item](./privilege_item.md) を参照してください。

## `db_admin`

`db_admin` は組み込みのデータベース管理者です。すべてのデータ関連の権限と、いくつかの基本的な運用および保守の権限を持っています。

- データベースとデータの管理に特化
- ユーザーまたはクラスター管理には利用不可
- 不変のロール

権限の範囲:

| 権限レベル        | 権限項目 |
| ----------------- | --------- |
| SYSTEM            | <ul><li>CREATE RESOURCE</li><li>PLUGIN</li><li>FILE</li><li>BLACKLIST</li><li>OPERATE</li><li>CREATE EXTERNAL CATALOG</li><li>REPOSITORY</li><li>CREATE RESOURCE GROUP</li><li>CREATE GLOBAL FUNCTION</li><li>CREATE STORAGE VOLUME</li><li>SECURITY</li></ul> |
| CATALOG           | <ul><li>USAGE</li><li>DROP</li><li>ALTER</li><li>CREATE DATABASE</li></ul> |
| DATABASE          | <ul><li>DROP</li><li>ALTER</li><li>CREATE TABLE</li><li>CREATE VIEW</li><li>CREATE MATERIALIZED VIEW</li><li>CREATE FUNCTION</li><li>CREATE PIPE</li><li>CREATE MASKING POLICY</li><li>CREATE ROW ACCESS POLICY</li></ul> |
| TABLE             | <ul><li>DROP</li><li>ALTER</li><li>INSERT</li><li>UPDATE</li><li>DELETE</li><li>SELECT</li><li>EXPORT</li></ul> |
| VIEW              | <ul><li>DROP</li><li>ALTER</li><li>SELECT</li><li>EXPORT</li></ul> |
| MATERIALIZED VIEW | <ul><li>DROP</li><li>ALTER</li><li>SELECT</li><li>REFRESH</li><li>EXPORT</li></ul> |
| RESOURCE          | <ul><li>USAGE</li><li>DROP</li><li>ALTER</li></ul> |
| RESOURCE GROUP    | <ul><li>DROP</li><li>ALTER</li></ul> |
| FUNCTION          | <ul><li>USAGE</li><li>DROP</li></ul> |
| GLOBAL FUNCTION   | <ul><li>USAGE</li><li>DROP</li></ul> |
| STORAGE VOLUME    | <ul><li>USAGE</li><li>DROP</li><li>ALTER</li></ul> |
| PIPE              | <ul><li>USAGE</li><li>DROP</li><li>ALTER</li></ul> |

## `cluster_admin`

`cluster_admin` は組み込みのクラスタ管理者です。

- クラスタインフラの管理に特化
- ノード管理の特権を付与
- 不変の役割

特権の範囲:

| 特権レベル       | 特権項目       |
| ---------------- | -------------- |
| SYSTEM           | NODE           |

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

`security_admin` はビルトインのセキュリティ管理者です。セキュリティ統合と Group Provider を管理するために使用できます。

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