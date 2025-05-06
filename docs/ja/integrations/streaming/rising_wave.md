---
displayed_sidebar: docs
---

# Sink data from RisingWave to StarRocks

RisingWave は、ストリーミングデータのシンプルで効率的、かつ信頼性の高い処理を可能にする分散型 SQL ストリーミングデータベースです。RisingWave のクイックスタートについては、 [Get started](https://docs.risingwave.com/docs/current/get-started/) を参照してください。

RisingWave は、ユーザーが他のサードパーティコンポーネントを必要とせずに直接 StarRocks にデータをシンクできるデータシンキング機能を提供します。この機能は、すべての StarRocks テーブルタイプ（Duplicate Key、主キーテーブル、Aggregate、ユニークキーテーブル）で動作します。

## 前提条件

- v1.7 以降の RisingWave クラスターが稼働していること。
- ターゲットの StarRocks テーブルにアクセスでき、StarRocks のバージョンが v2.5 以降であること。
- StarRocks テーブルにデータをシンクするには、ターゲットテーブルに対する SELECT および INSERT 権限が必要です。権限を付与するには、 [GRANT](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/account-management/GRANT/) を参照してください。

:::tip

RisingWave は StarRocks Sink に対して少なくとも一度のセマンティクスのみをサポートしています。これは、障害が発生した場合に重複したデータが書き込まれる可能性があることを意味します。データの重複排除とエンドツーエンドの冪等性のある書き込みを実現するために、 [StarRocks Primary Key tables](https://docs.starrocks.io/zh/docs/table_design/table_types/primary_key_table/) の使用を推奨します。

:::

## パラメータ

RisingWave から StarRocks にデータをシンクする際に設定する必要があるパラメータを以下の表に示します。特に指定がない限り、すべてのパラメータは必須です。

| パラメータ                                                       | 説明                                                  |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| connector                                                    | `starrocks` に設定します。                                      |
| starrocks.host                                               | StarRocks FE ノードの IP アドレス。      |
| starrocks.query_port                                         | FE ノードのクエリポート。          |
| starrocks.http_port                                          | FE ノードの HTTP ポート。                            |
| starrocks.user                                               | StarRocks クラスターにアクセスするためのユーザー名。  |
| starrocks.password                                           | ユーザー名に関連付けられたパスワード。        |
| starrocks.database                                           | ターゲットテーブルが存在する StarRocks データベース。                 |
| starrocks.table                                              | データをシンクしたい StarRocks テーブル。                           |
| starrocks.partial_update                                     | (オプション) StarRocks 部分更新機能を有効にするかどうか。この機能を有効にすると、更新が必要な列が少ない場合にシンクのパフォーマンスが向上します。  |
| type                                                         | シンク中のデータ操作タイプ。<ul><li>`append-only`: INSERT 操作のみを実行します。 </li><li>`upsert`: Upsert 操作を実行します。この設定を使用する場合、StarRocks ターゲットテーブルは主キーテーブルでなければなりません。 </li></ul>            |
| force_append_only                                            | (オプション) `type` が `append-only` に設定されているが、シンクプロセスに Upsert および Delete 操作が含まれている場合、この設定によりシンクタスクが append-only データを生成し、Upsert および Delete データを破棄することができます。 |
| primary_key                                                  | (オプション) StarRocks テーブルの主キー。`type` が `upsert` の場合に必要です。  |

## データ型のマッピング

以下の表は、RisingWave と StarRocks 間のデータ型のマッピングを示しています。

| RisingWave                                            | StarRocks|
| ----------------------------------------------------- | -------------- |
| BOOLEAN                                               | BOOLEAN        |
| SMALLINT                                              | SMALLINT       |
| INTEGER                                               | INT            |
| BIGINT                                                | BIGINT         |
| REAL                                                  | FLOAT          |
| DOUBLE                                                | DOUBLE         |
| DECIMAL                                               | DECIMAL        |
| DATE                                                  | DATE           |
| VARCHAR                                               | VARCHAR        |
| TIME <br />(StarRocks にシンクする前に VARCHAR にキャスト) | サポートされていません  |
| TIMESTAMP                                             | DATETIME       |
| TIMESTAMP WITH TIME ZONE <br />(StarRocks にシンクする前に TIMESTAMP にキャスト)  | サポートされていません  |
| INTERVAL <br />(StarRocks にシンクする前に VARCHAR にキャスト) | サポートされていません  |
| STRUCT                                                | JSON           |
| ARRAY                                                 | ARRAY          |
| BYTEA <br />(StarRocks にシンクする前に VARCHAR にキャスト)   | サポートされていません  |
| JSONB                                                 | JSON           |
| SERIAL                                                | BIGINT         |

## 例

1. StarRocks でデータベース `demo` を作成し、このデータベースに主キーテーブル `score_board` を作成します。

   ```sql
   CREATE DATABASE demo;
   USE demo;

   CREATE TABLE demo.score_board(
       id int(11) NOT NULL COMMENT "",
       name varchar(65533) NULL DEFAULT "" COMMENT "",
       score int(11) NOT NULL DEFAULT "0" COMMENT ""
   )
   PRIMARY KEY(id)
   DISTRIBUTED BY HASH(id);
   ```

2. RisingWave から StarRocks にデータをシンクします。

   ```sql
   -- RisingWave にテーブルを作成します。
   CREATE TABLE score_board (
       id INT PRIMARY KEY,
       name VARCHAR,
       score INT
   );
   
   -- テーブルにデータを挿入します。
   INSERT INTO score_board VALUES (1, 'starrocks', 100), (2, 'risingwave', 100);

   -- このテーブルから StarRocks テーブルにデータをシンクします。
   CREATE SINK score_board_sink
   FROM score_board WITH (
       connector = 'starrocks',
       type = 'upsert',
       starrocks.host = 'starrocks-fe',
       starrocks.mysqlport = '9030',
       starrocks.httpport = '8030',
       starrocks.user = 'users',
       starrocks.password = '123456',
       starrocks.database = 'demo',
       starrocks.table = 'score_board',
         primary_key = 'id'
   );
   ```