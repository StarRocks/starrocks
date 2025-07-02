---
displayed_sidebar: docs
toc_max_heading_level: 4
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# MaxCompute catalog

<Beta />

StarRocks は v3.3 以降、Alibaba Cloud MaxCompute (以前は ODPS として知られていました) の catalog をサポートしています。

MaxCompute catalog は、データを取り込むことなく MaxCompute からデータをクエリすることを可能にする一種の external catalog です。

MaxCompute catalog を使用すると、[INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) を使用して MaxCompute からデータを直接変換してロードすることもできます。

## 使用上の注意

MaxCompute catalog は、MaxCompute からデータをクエリするためにのみ使用できます。MaxCompute catalog を使用して、MaxCompute クラスター内のデータを削除、削除、または挿入することはできません。

## 統合準備

MaxCompute catalog を作成する前に、StarRocks クラスターが MaxCompute サービスに適切にアクセスできることを確認してください。

## MaxCompute catalog の作成

### 構文

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "odps",
    CatalogParams,
    ScanParams,
    CachingMetaParams
)
```

### パラメータ

#### catalog_name

MaxCompute catalog の名前です。命名規則は次のとおりです。

- 名前には文字、数字 (0-9)、およびアンダースコア (_) を含めることができます。文字で始める必要があります。
- 名前は大文字と小文字を区別し、長さは 1023 文字を超えることはできません。

#### comment

MaxCompute catalog の説明です。このパラメータはオプションです。

#### type

データソースのタイプです。値を `odps` に設定します。

#### CatalogParams

StarRocks が MaxCompute クラスターのメタデータにアクセスする方法に関する一連のパラメータです。

次の表は、`CatalogParams` で設定する必要があるパラメータを説明しています。

| パラメータ            | 必須  | 説明                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|----------------------|-------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| odps.endpoint        | はい   | MaxCompute サービスの接続アドレス (すなわち、エンドポイント) です。MaxCompute プロジェクトを作成する際に選択したリージョンおよびネットワーク接続モードに応じてエンドポイントを設定する必要があります。異なるリージョンおよびネットワーク接続モードで使用されるエンドポイントの詳細については、[Endpoint](https://www.alibabacloud.com/help/en/maxcompute/user-guide/endpoints) を参照してください。現在、Alibaba Cloud の 2 つのネットワーク接続モードのみがサポートされており、最良の体験を提供します: VPC およびクラシックネットワーク。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| odps.project         | はい   | アクセスしたい MaxCompute プロジェクトの名前です。標準モードのワークスペースを作成した場合、このパラメータを設定する際に本番環境と開発環境 (_dev) のプロジェクト名の違いに注意してください。[MaxCompute Console](https://account.alibabacloud.com/login/login.htm?spm=5176.12901015-2.0.0.593a525cwmiD7c) にログインし、**Workspace** > **Project Management** ページで MaxCompute プロジェクト名を取得できます。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| odps.access.id       | はい   | Alibaba Cloud アカウントまたは RAM ユーザーの AccessKey ID です。[AccessKey Management](https://ram.console.aliyun.com/manage/ak) ページにアクセスして AccessKey ID を取得できます。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| odps.access.key      | はい   | AccessKey ID に対応する AccessKey Secret です。[AccessKey Management](https://ram.console.aliyun.com/manage/ak) ページにアクセスして AccessKey Secret を取得できます。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| odps.tunnel.endpoint | いいえ | Tunnel サービスのパブリックネットワークアクセスリンクです。Tunnel エンドポイントを設定していない場合、Tunnel は自動的に MaxCompute サービスが所在するネットワークに一致する Tunnel エンドポイントにルーティングします。Tunnel エンドポイントを設定している場合、設定されたエンドポイントが使用され、自動ルーティングされません。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| odps.tunnel.quota    | はい   | MaxCompute にアクセスするために使用されるクォータの名前です。MaxCompute はデータ転送のために 2 種類のリソースを提供します: MaxCompute Tunnel 用の専用リソースグループ (サブスクリプション) とストレージ API (従量課金制)。リソースタイプに基づいてクォータ名を取得するために、次の操作を実行できます。<p/> **MaxCompute Tunnel 用の専用リソースグループ**: [MaxCompute console](https://maxcompute.console.aliyun.com/) にログインします。トップナビゲーションバーでリージョンを選択します。左側のナビゲーションペインで Workspace > Quotas を選択して、利用可能なクォータを表示します。詳細については、[Manage quotas for computing resources in the MaxCompute console](https://help.aliyun.com/zh/maxcompute/user-guide/manage-quotas-in-the-maxcompute-console) を参照してください。<p/> **Storage API**: [MaxCompute console](https://maxcompute.console.aliyun.com/) にログインします。左側のナビゲーションペインで Tenants > Tenant Property を選択します。Tenants ページで Storage API Switch をオンにします。詳細については、[Use storage API (pay-as-you-go)](https://help.aliyun.com/zh/maxcompute/user-guide/overview-1) を参照してください。ストレージ API のデフォルト名は **"pay-as-you-go"** です。 |

#### ScanParams

StarRocks が MaxCompute クラスターに保存されているファイルにアクセスする方法に関する一連のパラメータです。このパラメータセットはオプションです。

次の表は、`ScanParams` で設定する必要があるパラメータを説明しています。

| パラメータ            | 必須  | 説明                                       |
|----------------------|-------|-------------------------------------------|
| odps.split.policy    | いいえ | データスキャン時に使用されるシャードポリシーです。<br />有効な値: `size` (データサイズでシャード) および `row_offset` (行数でシャード)。デフォルト値: `size`。<br /> |
| odps.split.row.count | いいえ | `odps.split.policy` が `row_offset` に設定されている場合のシャードごとの最大行数です。<br />デフォルト値: `4 * 1024 * 1024 = 4194304`。<br />  |

#### CachingMetaParams

StarRocks が Hive のメタデータをキャッシュする方法に関する一連のパラメータです。このパラメータセットはオプションです。

次の表は、`CachingMetaParams` で設定する必要があるパラメータを説明しています。

| パラメータ                    | 必須  | 説明                                                                       |
|------------------------------|-------|---------------------------------------------------------------------------|
| odps.cache.table.enable      | いいえ | StarRocks が MaxCompute テーブルのメタデータをキャッシュするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `true`。値 `true` はキャッシュを有効にし、値 `false` はキャッシュを無効にします。                         |
| odps.cache.table.expire      | いいえ | StarRocks が MaxCompute テーブルまたはパーティションのキャッシュされたメタデータを自動的に削除する時間間隔 (秒単位) です。デフォルト値: `86400` (24 時間)。                                                                 |
| odps.cache.table.size        | いいえ | StarRocks がキャッシュする MaxCompute テーブルのメタデータエントリの数です。デフォルト値: `1000`。      |
| odps.cache.partition.enable  | いいえ | StarRocks が MaxCompute テーブルのすべてのパーティションのメタデータをキャッシュするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `true`。値 `true` はキャッシュを有効にし、値 `false` はキャッシュを無効にします。 |
| odps.cache.partition.expire  | いいえ | StarRocks が MaxCompute テーブルのすべてのパーティションのキャッシュされたメタデータを自動的に削除する時間間隔 (秒単位) です。デフォルト値: `86400` (24 時間)。                                                            |
| odps.cache.partition.size    | いいえ | StarRocks がすべてのパーティションのメタデータをキャッシュする MaxCompute テーブルの数です。デフォルト値: `1000`。 |
| odps.cache.table-name.enable | いいえ | StarRocks が MaxCompute プロジェクトからのテーブル情報をキャッシュするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。値 `true` はキャッシュを有効にし、値 `false` はキャッシュを無効にします。     |
| odps.cache.table-name.expire | いいえ | StarRocks が MaxCompute プロジェクトからのテーブル情報を自動的に削除する時間間隔 (秒単位) です。デフォルト値: `86400` (24 時間)。                                                                  |
| odps.cache.table-name.size   | いいえ | StarRocks がキャッシュする MaxCompute プロジェクトの数です。デフォルト値: `1000`。   |

### 例

次の例は、`odps_project` をウェアハウスプロジェクトとして使用する `odps_catalog` という名前の MaxCompute catalog を作成します。

```SQL
CREATE EXTERNAL CATALOG odps_catalog 
PROPERTIES (
   "type"="odps",
   "odps.access.id"="<maxcompute_user_access_id>",
   "odps.access.key"="<maxcompute_user_access_key>",
   "odps.endpoint"="<maxcompute_server_endpoint>",
   "odps.project"="odps_project"
);
```

## MaxCompute catalog の表示

現在の StarRocks クラスター内のすべての catalog をクエリするには、[SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用できます。

```SQL
SHOW CATALOGS;
```

また、[SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用して external catalog の作成ステートメントをクエリすることもできます。次の例は、`odps_catalog` という名前の MaxCompute catalog の作成ステートメントをクエリします。

```SQL
SHOW CREATE CATALOG odps_catalog;
```

## MaxCompute catalog の削除

external catalog を削除するには、[DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用できます。

次の例は、`odps_catalog` という名前の MaxCompute catalog を削除します。

```SQL
DROP CATALOG odps_catalog;
```

## MaxCompute テーブルのスキーマを表示

MaxCompute テーブルのスキーマを表示するには、次のいずれかの構文を使用できます。

- スキーマを表示

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>;
  ```

- CREATE ステートメントからスキーマと場所を表示

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>;
  ```

## MaxCompute テーブルをクエリ

1. MaxCompute クラスター内のデータベースを表示するには、[SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用します。

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. 現在のセッションで目的の catalog に切り替えるには、[SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用します。

   ```SQL
   SET CATALOG <catalog_name>;
   ```

   次に、現在のセッションでアクティブなデータベースを指定するには、[USE](../../sql-reference/sql-statements/Database/USE.md) を使用します。

   ```SQL
   USE <db_name>;
   ```

   または、目的の catalog でアクティブなデータベースを直接指定するには、[USE](../../sql-reference/sql-statements/Database/USE.md) を使用します。

   ```SQL
   USE <catalog_name>.<db_name>;
   ```

3. 指定されたデータベース内の目的のテーブルをクエリするには、[SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を使用します。

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10;
   ```

## MaxCompute からデータをロード

StarRocks クラスターに `olap_tbl` という名前の OLAP テーブルがあり、MaxCompute クラスターに `mc_table` という名前のテーブルがあるとします。MaxCompute テーブル `mc_table` から StarRocks テーブル `olap_tbl` にデータを変換してロードするには、次のようにします。

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM mc_table;
```

## データ型のマッピング

MaxCompute catalog は、MaxCompute のデータ型を StarRocks のデータ型にマッピングします。次の表は、MaxCompute のデータ型と StarRocks のデータ型のマッピングを示しています。

| MaxCompute データ型  | StarRocks データ型 |
|-----------------------|---------------------|
| BOOLEAN               | BOOLEAN             |
| TINYINT               | TINYINT             |
| SMALLINT              | SMALLINT            |
| INT                   | INT                 |
| BIGINT                | BIGINT              |
| FLOAT                 | FLOAT               |
| DOUBLE                | DOUBLE              |
| DECIMAL(p, s)         | DECIMAL(p, s)       |
| STRING                | VARCHAR(1073741824) |
| VARCHAR(n)            | VARCHAR(n)          |
| CHAR(n)               | CHAR(n)             |
| JSON                  | VARCHAR(1073741824) |
| BINARY                | VARBINARY           |
| DATE                  | DATE                |
| DATETIME              | DATETIME            |
| TIMESTAMP             | DATETIME            |
| ARRAY                 | ARRAY               |
| MAP                   | MAP                 |
| STRUCT                | STRUCT              |

:::note

TIMESTAMP 型は、StarRocks での型変換により精度が失われます。

:::

## CBO 統計の収集

現在のバージョンでは、MaxCompute catalog は MaxCompute テーブルの CBO 統計を自動的に収集できないため、オプティマイザが最適なクエリプランを生成できない可能性があります。そのため、MaxCompute テーブルの CBO 統計を手動でスキャンして StarRocks にインポートすることで、クエリを効果的に高速化できます。

MaxCompute クラスターに `mc_table` という名前の MaxCompute テーブルがあるとします。[ANALYZE TABLE](../../sql-reference/sql-statements/cbo_stats/ANALYZE_TABLE.md) を使用して CBO 統計を収集するための手動収集タスクを作成できます。

```SQL
ANALYZE TABLE mc_table;
```

## メタデータキャッシュの手動更新

デフォルトでは、StarRocks はクエリパフォーマンスを向上させるために MaxCompute のメタデータをキャッシュします。そのため、スキーマ変更や MaxCompute テーブルへのその他の更新を行った後、[REFRESH EXTERNAL TABLE](../../sql-reference/sql-statements/table_bucket_part_index/REFRESH_EXTERNAL_TABLE.md) を使用してテーブルのメタデータを手動で更新し、StarRocks が最新のメタデータを迅速に取得できるようにします。

```SQL
REFRESH EXTERNAL TABLE <table_name> [PARTITION ('partition_name', ...)]
```