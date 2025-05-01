---
displayed_sidebar: docs
---

# Information Schema

StarRocks の `information_schema` は、各 StarRocks インスタンス内のデータベースです。`information_schema` には、StarRocks インスタンスが管理するすべてのオブジェクトの広範なメタデータ情報を格納する、いくつかの読み取り専用のシステム定義テーブルが含まれています。

## Information Schema を介したメタデータの表示

StarRocks インスタンス内のメタデータ情報は、`information_schema` 内のテーブルの内容をクエリすることで表示できます。

次の例では、StarRocks 内の `sr_member` という名前のテーブルについてのメタデータ情報を、`tables` テーブルをクエリすることで表示します。

```Plain
mysql> SELECT * FROM information_schema.tables WHERE TABLE_NAME like 'sr_member'\G
*************************** 1. row ***************************
  TABLE_CATALOG: def
   TABLE_SCHEMA: sr_hub
     TABLE_NAME: sr_member
     TABLE_TYPE: BASE TABLE
         ENGINE: StarRocks
        VERSION: NULL
     ROW_FORMAT: NULL
     TABLE_ROWS: 6
 AVG_ROW_LENGTH: 542
    DATA_LENGTH: 3255
MAX_DATA_LENGTH: NULL
   INDEX_LENGTH: NULL
      DATA_FREE: NULL
 AUTO_INCREMENT: NULL
    CREATE_TIME: 2022-11-17 14:32:30
    UPDATE_TIME: 2022-11-17 14:32:55
     CHECK_TIME: NULL
TABLE_COLLATION: utf8_general_ci
       CHECKSUM: NULL
 CREATE_OPTIONS: NULL
  TABLE_COMMENT: OLAP
1 row in set (1.04 sec)
```

## Information Schema テーブル

StarRocks は、`information_schema` 内の次のテーブルによって提供されるメタデータ情報を最適化しました。

| **Information Schema table name** | **Description**                                              |
| --------------------------------- | ------------------------------------------------------------ |
| [tables](#tables)                            | テーブルの一般的なメタデータ情報を提供します。             |
| [tables_config](#tables_config)                     | StarRocks に固有の追加のテーブルメタデータ情報を提供します。 |

### tables

`tables` には次のフィールドが提供されています。

| **Field**       | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| TABLE_CATALOG   | テーブルを格納するカタログの名前。                           |
| TABLE_SCHEMA    | テーブルを格納するデータベースの名前。                       |
| TABLE_NAME      | テーブルの名前。                                             |
| TABLE_TYPE      | テーブルのタイプ。 有効な値: "BASE TABLE" または "VIEW"。   |
| ENGINE          | テーブルのエンジンタイプ。 有効な値: "StarRocks", "MySQL", "MEMORY" または空文字列。 |
| VERSION         | StarRocks で利用できない機能に適用されます。                 |
| ROW_FORMAT      | StarRocks で利用できない機能に適用されます。                 |
| TABLE_ROWS      | テーブルの行数。                                             |
| AVG_ROW_LENGTH  | テーブルの平均行長（サイズ）。`DATA_LENGTH` / `TABLE_ROWS` に相当します。単位: バイト。 |
| DATA_LENGTH     | テーブルのデータ長（サイズ）。単位: バイト。                 |
| MAX_DATA_LENGTH | StarRocks で利用できない機能に適用されます。                 |
| INDEX_LENGTH    | StarRocks で利用できない機能に適用されます。                 |
| DATA_FREE       | StarRocks で利用できない機能に適用されます。                 |
| AUTO_INCREMENT  | StarRocks で利用できない機能に適用されます。                 |
| CREATE_TIME     | テーブルが作成された時間。                                   |
| UPDATE_TIME     | テーブルが最後に更新された時間。                             |
| CHECK_TIME      | テーブルに対して整合性チェックが最後に実行された時間。       |
| TABLE_COLLATION | テーブルのデフォルトの照合順序。                             |
| CHECKSUM        | StarRocks で利用できない機能に適用されます。                 |
| CREATE_OPTIONS  | StarRocks で利用できない機能に適用されます。                 |
| TABLE_COMMENT   | テーブルに関するコメント。                                   |

### tables_config

`tables_config` には次のフィールドが提供されています。

| **Field**        | **Description**                                              |
| ---------------- | ------------------------------------------------------------ |
| TABLE_SCHEMA     | テーブルを格納するデータベースの名前。                       |
| TABLE_NAME       | テーブルの名前。                                             |
| TABLE_ENGINE     | テーブルのエンジンタイプ。                                   |
| TABLE_MODEL      | テーブルタイプ。 有効な値: "DUP_KEYS", "AGG_KEYS", "UNQ_KEYS" または "PRI_KEYS"。 |
| PRIMARY_KEY      | 主キーテーブルまたはユニークキーテーブルの主キー。テーブルが主キーテーブルまたはユニークキーテーブルでない場合は空文字列が返されます。 |
| PARTITION_KEY    | テーブルのパーティション列。                                 |
| DISTRIBUTE_KEY   | テーブルのバケット列。                                       |
| DISTRIBUTE_TYPE  | テーブルのデータ分散方法。                                   |
| DISTRIBUTE_BUCKET | テーブルのバケット数。                                       |
| SORT_KEY         | テーブルのソートキー。                                       |
| PROPERTIES       | テーブルのプロパティ。                                       |
| TABLE_ID         | テーブルの ID。                                              |

## load_tracking_logs

この機能は StarRocks v3.0 からサポートされています。

`load_tracking_logs` には次のフィールドが提供されています。

| **Field**     | **Description**                                                                       |
|---------------|---------------------------------------------------------------------------------------|
| JOB_ID        | ロードジョブの ID。                                                                   |
| LABEL         | ロードジョブのラベル。                                                                |
| DATABASE_NAME | ロードジョブが属するデータベース。                                                    |
| TRACKING_LOG  | ロードジョブのエラーログ（存在する場合）。                                            |
| Type          | ロードジョブのタイプ。 有効な値: BROKER, INSERT, ROUTINE_LOAD および STREAM_LOAD。     |

## materialized_views

`materialized_views` には次のフィールドが提供されています。

| **Field**                            | **Description**                                              |
| ------------------------------------ | ------------------------------------------------------------ |
| MATERIALIZED_VIEW_ID                 | マテリアライズドビューの ID                                   |
| TABLE_SCHEMA                         | マテリアライズドビューが存在するデータベース                  |
| TABLE_NAME                           | マテリアライズドビューの名前                                  |
| TABLE_ROWS                           | マテリアライズドビューのデータ行数（おおよそのバックグラウンド統計に基づく） |
| MATERIALIZED_VIEW_DEFINITION         | マテリアライズドビューの SQL 定義                             |