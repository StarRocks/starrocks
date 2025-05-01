---
displayed_sidebar: docs
unlisted: True
---

# テーブルを作成する

このクイックスタートチュートリアルでは、StarRocks でテーブルを作成するための必要な手順を説明し、StarRocks の基本的な機能を紹介します。

StarRocks インスタンスがデプロイされた後（詳細は [Deploy StarRocks](../quick_start/deploy_with_docker.md) を参照）、データベースとテーブルを作成して [データをロードおよびクエリ](../quick_start/Import_and_query.md) する必要があります。データベースとテーブルの作成には、対応する [ユーザー権限](../administration/user_privs/User_privilege.md) が必要です。このクイックスタートチュートリアルでは、StarRocks インスタンスで最高の権限を持つデフォルトの `root` ユーザーを使用して、以下の手順を実行できます。

> **注意**
>
> このチュートリアルは、既存の StarRocks インスタンス、データベース、テーブル、およびユーザー権限を使用して完了することもできます。ただし、簡単のために、チュートリアルで提供されるスキーマとデータを使用することをお勧めします。

## ステップ 1: StarRocks にログインする

MySQL クライアントを介して StarRocks にログインします。デフォルトのユーザー `root` でログインでき、パスワードはデフォルトで空です。

```Plain
mysql -h <fe_ip> -P<fe_query_port> -uroot
```

> **注意**
>
> - 異なる FE MySQL サーバーポート (`query_port`, デフォルト: `9030`) を割り当てた場合は、`-P` の値を適宜変更してください。
> - FE 設定ファイルで `priority_networks` の設定項目を指定した場合は、`-h` の値を適宜変更してください。

## ステップ 2: データベースを作成する

[CREATE DATABASE](../sql-reference/sql-statements/Database//CREATE_DATABASE.md) を参照して、`sr_hub` という名前のデータベースを作成します。

```SQL
CREATE DATABASE IF NOT EXISTS sr_hub;
```

この StarRocks インスタンス内のすべてのデータベースを表示するには、[SHOW DATABASES](../sql-reference/sql-statements/Database/SHOW_DATABASES.md) SQL を実行します。

## ステップ 3: テーブルを作成する

`USE sr_hub` を実行して `sr_hub` データベースに切り替え、[CREATE TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) を参照して `sr_member` という名前のテーブルを作成します。

```SQL
USE sr_hub;
CREATE TABLE IF NOT EXISTS sr_member (
    sr_id            INT,
    name             STRING,
    city_code        INT,
    reg_date         DATE,
    verified         BOOLEAN
)
PARTITION BY RANGE(reg_date)
(
    PARTITION p1 VALUES [('2022-03-13'), ('2022-03-14')),
    PARTITION p2 VALUES [('2022-03-14'), ('2022-03-15')),
    PARTITION p3 VALUES [('2022-03-15'), ('2022-03-16')),
    PARTITION p4 VALUES [('2022-03-16'), ('2022-03-17')),
    PARTITION p5 VALUES [('2022-03-17'), ('2022-03-18'))
)
DISTRIBUTED BY HASH(city_code);
```

> **注意**
>
> - バージョン 3.1 以降、テーブルを作成する際に DISTRIBUTED BY 句でバケッティングキーを指定する必要はありません。StarRocks はランダムバケット法をサポートしており、データをすべてのバケットにランダムに分散させます。詳細は [Random bucketing](../table_design/data_distribution/Data_distribution.md#random-bucketing-since-v31) を参照してください。
> - デプロイした StarRocks インスタンスには 1 つの BE ノードしかないため、データレプリカの数を表すテーブルプロパティ `replication_num` を `1` として指定する必要があります。
> - [テーブルタイプ](../table_design/table_types/table_types.md) が指定されていない場合、デフォルトで重複キーテーブルが作成されます。[Duplicate Key table](../table_design/table_types/duplicate_key_table.md) を参照してください。
> - テーブルの列は、チュートリアルで StarRocks にロードするデータのフィールドに正確に対応しています。[データのロードとクエリ](../quick_start/Import_and_query.md) に関するチュートリアルを参照してください。
> - **本番環境での高パフォーマンス** を保証するために、`PARTITION BY` 句を使用してテーブルのデータパーティショニング計画を戦略的に立てることを強くお勧めします。[Design partitioning and bucketing rules](../table_design/data_distribution/Data_distribution.md#design-partitioning-and-bucketing-rules) を参照して、詳細な指示を確認してください。

テーブルが作成された後、DESC ステートメントを使用してテーブルの詳細を確認し、[SHOW TABLES](../sql-reference/sql-statements/table_bucket_part_index/SHOW_TABLES.md) を実行してデータベース内のすべてのテーブルを表示できます。StarRocks のテーブルはスキーマ変更をサポートしています。詳細は [ALTER TABLE](../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) を参照してください。

## 次に何をするか

StarRocks テーブルの概念的な詳細について学ぶには、[StarRocks Table Design](../table_design/StarRocks_table_design.md) を参照してください。

このチュートリアルで示された機能に加えて、StarRocks は以下もサポートしています:

- 様々な [データタイプ](../sql-reference/data-types/numeric/BIGINT.md)
- 複数の [テーブルタイプ](../table_design/table_types/table_types.md)
- 柔軟な [パーティショニング戦略](../table_design/data_distribution/Data_distribution.md#dynamic-partition-management)
- クラシックなデータベースクエリインデックス、[bitmap index](../using_starrocks/Bitmap_index.md) や [bloom filter index](../using_starrocks/Bloomfilter_index.md) を含む
- [マテリアライズドビュー](../using_starrocks/async_mv/Materialized_view.md)