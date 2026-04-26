---
displayed_sidebar: docs
---

import Experimental from '../_assets/commonMarkdown/_experimental.mdx'

# dbt

`dbt-starrocks` は、`dbt` を使用して StarRocks 内のデータを変換するためのツールで、dbt のモデリングパターンとベストプラクティスを活用できます。

`dbt-starrocks` [GitHub repo](https://github.com/StarRocks/dbt-starrocks).

<Experimental />

## Supported features

## Supported features

| StarRocks >= 3.1 | StarRocks >= 3.4 |              Feature              |
|:----------------:|:----------------:|:---------------------------------:|
|        ✅        |        ✅         |       Table materialization       |
|        ✅        |        ✅         |       View materialization        |
|        ✅        |        ✅         | Materialized View materialization |
|        ✅        |        ✅         |    Incremental materialization    |
|        ✅        |        ✅         |         Primary Key Model         |
|        ✅        |        ✅         |              Sources              |
|        ✅        |        ✅         |         Custom data tests         |
|        ✅        |        ✅         |           Docs generate           |
|        ✅        |        ✅         |       Expression Partition        |
|        ❌        |        ❌         |               Kafka               |
|        ❌        |        ✅         |         Dynamic Overwrite         |
|        `*`       |        ✅         |            Submit task            |
|        ✅        |        ✅         |  Microbatch (Insert Overwrite)   |
|        ❌        |        ✅         | Microbatch (Dynamic Overwrite)   |

`*` 特定の `submit task` のサポートを確認するには、[SUBMIT TASK](../sql-reference/sql-statements/loading_unloading/ETL/SUBMIT_TASK.md) を参照してください。

## Installation

StarRocks DBT アダプターを pip を使用してインストールします。

```sh
pip install dbt-starrocks
```

## Verify Installation

インストールを確認するには、バージョンをチェックします。

```sh
dbt --version
```

これにより、`starrocks` がプラグインとしてリストされるはずです。

## Configuration

### Profiles

StarRocks 固有の設定で `profiles.yml` を作成または更新します。

```yaml
starrocks_project:
  target: dev
  outputs:
    dev:
      type: starrocks
      host: your-starrocks-host.com
      port: 9030
      schema: your_database
      username: your_username
      password: your_password
      catalog: test_catalog
```

### Parameters

#### `type`

**Description**: 使用する特定のアダプターで、これは `starrocks` に設定する必要があります\
**Required\?**: 必須\
**Example**: `starrocks`

#### `host`

**Description**: 接続するホスト名\
**Required\?**: 必須\
**Example**: `192.168.100.28`

#### `port`

**Description**: 使用するポート\
**Required\?**: 必須\
**Example**: `9030`

#### `catalog`

**Description**: モデルを構築するカタログを指定\
**Required\?**: 任意\
**Example**: `default_catalog`

#### `schema`

**Description**: モデルを構築するスキーマ (StarRocks のデータベース) を指定\
**Required\?**: 必須\
**Example**: `analytics`

#### `username`

**Description**: サーバーに接続するために使用するユーザー名\
**Required\?**: 必須\
**Example**: `dbt_admin`

#### `password`

**Description**: サーバーへの認証に使用するパスワード\
**Required\?**: 必須\
**Example**: `correct-horse-battery-staple` 

#### `version`

**Description**: プラグインが互換性のある starrocks バージョンに移行しようとする\
**Required\?**: 任意\
**Example**: `3.1.0`

#### `use_pure`

**Description**: C 拡張を使用するには "true" に設定\
**Required\?**: 任意\
**Example**: `true`

#### `is_async`

**Description**: 適切なタスクを etl タスクとして送信するには "true"\
**Required\?**: 任意\
**Example**: `true`

#### `async_query_timeout`

**Description**: StarRocks にタスクを送信する際の `query_timeout` 値を設定\
**Required\?**: 任意\
**Example**: `300`

### Sources

`sources.yml` を作成または更新します。

```yml
sources:
  - name: your_source
    database: your_sr_catalog
    schema: your_sr_database
    tables:
      - name: your_table
```

スキーマでカタログが指定されていない場合、プロファイルで定義されたカタログがデフォルトになります。前述のプロファイルを使用すると、カタログが定義されていない場合、モデルはソースが `test_catalog.your_sr_database` にあると仮定します。

## Materializations

### Table

基本的なテーブル設定

```sql
{{ config(
    materialized='table',
    engine='OLAP',
    keys=['id', 'name', 'created_date'],
    table_type='PRIMARY',
    distributed_by=['id'],
    buckets=3,
    partition_by=['created_date'],
    properties=[
        {"replication_num": "1"}
    ]
) }}

SELECT 
    id,
    name,
    email,
    created_date,
    last_modified_date
FROM {{ source('your_source', 'users') }}
```

## Configuration Options

- engine: ストレージエンジン (デフォルト: `OLAP`)
- keys: ソートキーを定義するカラム
- table_type: テーブルモデルタイプ 
  - `PRIMARY`: プライマリキーのモデル (アップサートと削除をサポート)
  - `DUPLICATE`: 重複キーのモデル (重複行を許可)
  - `UNIQUE`: ユニークキーのモデル (一意性を強制)
- `distributed_by`: ハッシュ分散のためのカラム
- `buckets`: データ分散のためのバケット数 (自動バケット化の場合は空のままにする)
- `partition_by`: テーブルパーティションのためのカラム
- `partition_by_init`: 初期パーティション定義
- `properties`: 追加の StarRocks テーブルプロパティ

## Tables in External Catalogs

### Read from External into StarRocks

この例では、外部の Hive カタログから集計データを含むマテリアライズドテーブルを StarRocks に作成します。

:::tip
外部カタログがまだ存在しない場合は、設定してください。

```sql
CREATE EXTERNAL CATALOG `hive_external`
PROPERTIES (
    "hive.metastore.uris"  =  "thrift://127.0.0.1:8087",
    "type"="hive"
);
```
:::

```sql
{{ config(
    materialized='table',
    keys=['product_id', 'order_date'],
    distributed_by=['product_id'],
    partition_by=['order_date']
) }}

-- Hive 外部カタログから StarRocks テーブルへの集計データ
SELECT 
    h.product_id,
    h.order_date,
    COUNT(*) as order_count,
    SUM(h.amount) as total_amount,
    MAX(h.last_updated) as last_updated
FROM {{ source('hive_external', 'orders') }} h
GROUP BY 
    h.product_id,
    h.order_date
```

### Write to External

```sql
{{
  config(
    materialized='table',
    on_table_exists = 'replace',
    partition_by=['order_date'],
    properties={},
    catalog='external_catalog',
    database='test_db'
  )
}}

SELECT * FROM {{ source('iceberg_external', 'orders') }}
```

外部カタログへのマテリアライゼーションの設定は、少ないオプションをサポートします。`on_table_exists`、`partition_by`、および `properties` がサポートされています。`catalog` と `database` が設定されていない場合、プロファイルからのデフォルトが使用されます。

### Incremental

インクリメンタルマテリアライゼーションも StarRocks でサポートされています。

```sql
{{ config(
    materialized='incremental',
    unique_key='id',
    table_type='PRIMARY',
    keys=['id'],
    distributed_by=['id'],
    incremental_strategy='default'
) }}

SELECT
    id,
    user_id,
    event_name,
    event_timestamp,
    properties
FROM {{ source('raw', 'events') }}

{% if is_incremental() %}
    WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}
```

#### Incremental Strategies

`dbt-starrocks` は複数のインクリメンタル戦略をサポートしています。

1. `append` (デフォルト): 重複排除なしで新しいレコードを単純に追加
2. `insert_overwrite`: 挿入でテーブルパーティションを上書き
3. `dynamic_overwrite`: テーブルパーティションを上書き、作成、および書き込み

どの上書き戦略を使用するかについての詳細は、[INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md) ドキュメントを参照してください。

:::note
現在、インクリメンタルマージはサポートされていません。
:::

## Troubleshooting

- dbt で外部カタログを使用する前に、StarRocks でそれらを作成する必要があります。詳細は [こちら](../data_source/catalog/catalog_overview.md) のドキュメントを参照してください。
- 外部ソースは `{{ source('external_source_name', 'table_name' }}` マクロを使用してアクセスする必要があります。
- `dbt seed` は外部カタログではテストされておらず、現在サポートされていません。
- 現在存在しない外部データベースに `dbt` がモデルを作成するためには、プロパティを通じてモデルの場所を設定する必要があります。
- 外部モデルは保存される場所を定義する必要があります。この場所は、宛先データベースが存在し、場所プロパティを設定する場合に定義されます。それ以外の場合、場所を設定する必要があります。
  - 現在、既に存在するデータベースに外部モデルを作成することのみをサポートします。