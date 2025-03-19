---
displayed_sidebar: docs
---

# REFRESH EXTERNAL TABLE

## 説明

StarRocks にキャッシュされたメタデータを更新します。このメタデータはデータレイクのテーブルからのものです。このステートメントは以下のシナリオで使用されます:

- **外部テーブル**: Apache Hive™ または Apache Hudi のデータをクエリするために Hive 外部テーブルまたは Hudi 外部テーブルを使用する場合、StarRocks にキャッシュされた Hive テーブルまたは Hudi テーブルのメタデータを更新するためにこのステートメントを実行できます。
- **外部カタログ**: 対応するデータソースのクエリに[外部カタログ](../../../data_source/catalog/catalog_overview.md)を使用する場合、StarRocks にキャッシュされたメタデータを更新するためにこのステートメントを実行できます。

## 基本概念

- **Hive 外部テーブル**: StarRocks に作成および保存されます。Hive データをクエリするために使用できます。
- **Hudi 外部テーブル**: StarRocks に作成および保存されます。Hudi データをクエリするために使用できます。
- **Hive テーブル**: Hive に作成および保存されます。
- **Hudi テーブル**: Hudi に作成および保存されます。

## 構文とパラメータ

異なるケースに基づく構文とパラメータは以下の通りです:

- 外部テーブル

    ```SQL
    REFRESH EXTERNAL TABLE table_name 
    [PARTITION ('partition_name', ...)]
    ```

    | **パラメータ**  | **必須** | **説明**                                              |
    | -------------- | -------- | ---------------------------------------------------- |
    | table_name     | はい     | Hive 外部テーブルまたは Hudi 外部テーブルの名前。    |
    | partition_name | いいえ   | Hive テーブルまたは Hudi テーブルのパーティションの名前。このパラメータを指定すると、StarRocks にキャッシュされた Hive テーブルおよび Hudi テーブルのパーティションのメタデータが更新されます。 |

- 外部カタログ

    ```SQL
    REFRESH EXTERNAL TABLE [external_catalog.][db_name.]table_name
    [PARTITION ('partition_name', ...)]
    ```

    | **パラメータ**    | **必須** | **説明**                                              |
    | ---------------- | -------- | ---------------------------------------------------- |
    | external_catalog | いいえ   | 外部カタログの名前。Hive、Hudi、Delta Lake、MaxCompute (v3.3 以降) のカタログをサポートします。 |
    | db_name          | いいえ   | 対象テーブルが存在するデータベースの名前。             |
    | table_name       | はい     | テーブルの名前。                                      |
    | partition_name   | いいえ   | パーティションの名前。このパラメータを指定すると、StarRocks にキャッシュされた対象テーブルのパーティションのメタデータが更新されます。 |

## 使用上の注意

`ALTER_PRIV` 権限を持つユーザーのみが、このステートメントを実行して StarRocks にキャッシュされた Hive テーブルおよび Hudi テーブルのメタデータを更新できます。

## 例

異なるケースでの使用例は以下の通りです:

### 外部テーブル

例 1: StarRocks にキャッシュされた対応する Hive テーブルのメタデータを、外部テーブル `hive1` を指定して更新します。

```SQL
REFRESH EXTERNAL TABLE hive1;
```

例 2: StarRocks にキャッシュされた対応する Hudi テーブルのパーティションのメタデータを、外部テーブル `hudi1` とそのテーブル内のパーティションを指定して更新します。

```SQL
REFRESH EXTERNAL TABLE hudi1
PARTITION ('date=2022-12-20', 'date=2022-12-21');
```

### 外部カタログ

例 1: StarRocks にキャッシュされた `hive_table` のメタデータを更新します。

```SQL
REFRESH EXTERNAL TABLE hive_catalog.hive_db.hive_table;
```

または

```SQL
USE hive_catalog.hive_db;
REFRESH EXTERNAL TABLE hive_table;
```

例 2: StarRocks にキャッシュされた `hive_table` の二次パーティション `p2` のメタデータを更新します。

```SQL
USE hive_catalog.hive_db;
REFRESH EXTERNAL TABLE hive_table PARTITION ('p1=${date}/p2=${hour}');
```

例 3: StarRocks にキャッシュされた `hudi_table` のパーティションのメタデータを更新します。

```SQL
REFRESH EXTERNAL TABLE hudi_catalog.hudi_db.hudi_table
PARTITION ('date=2022-12-20', 'date=2022-12-21');
```