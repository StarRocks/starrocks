---
displayed_sidebar: docs
---

# REFRESH EXTERNAL TABLE

## 説明

StarRocks にキャッシュされた Hive と Hudi のメタデータを更新します。このステートメントは、次のいずれかのシナリオで使用されます。

- **外部テーブル**: Apache Hive™ または Apache Hudi のデータをクエリするために Hive 外部テーブルまたは Hudi 外部テーブルを使用する場合、StarRocks にキャッシュされた Hive テーブルまたは Hudi テーブルのメタデータを更新するためにこのステートメントを実行できます。
- **外部カタログ**: Hive または Hudi のデータをクエリするために [Hive catalog](../../../data_source/catalog/hive_catalog.md) または [Hudi catalog](../../../data_source/catalog/hudi_catalog.md) を使用する場合、StarRocks にキャッシュされた Hive テーブルまたは Hudi テーブルのメタデータを更新するためにこのステートメントを実行できます。

## 基本概念

- **Hive 外部テーブル**: StarRocks に作成および保存されます。Hive データをクエリするために使用できます。
- **Hudi 外部テーブル**: StarRocks に作成および保存されます。Hudi データをクエリするために使用できます。
- **Hive テーブル**: Hive に作成および保存されます。
- **Hudi テーブル**: Hudi に作成および保存されます。

## 構文とパラメータ

異なるケースに基づく構文とパラメータは次のとおりです。

- 外部テーブル

    ```SQL
    REFRESH EXTERNAL TABLE table_name 
    [PARTITION ('partition_name', ...)]
    ```

    | **パラメータ**  | **必須** | **説明**                                              |
    | -------------- | ------------ | ------------------------------------------------------------ |
    | table_name     | Yes          | Hive 外部テーブルまたは Hudi 外部テーブルの名前。    |
    | partition_name | No           | Hive テーブルまたは Hudi テーブルのパーティションの名前。このパラメータを指定すると、StarRocks にキャッシュされた Hive テーブルおよび Hudi テーブルのパーティションのメタデータが更新されます。 |

- 外部カタログ

    ```SQL
    REFRESH EXTERNAL TABLE [external_catalog.][db_name.]table_name
    [PARTITION ('partition_name', ...)]
    ```

    | **パラメータ**    | **必須** | **説明**                                              |
    | ---------------- | ------------ | ------------------------------------------------------------ |
    | external_catalog | No           | Hive catalog または Hudi catalog の名前。                  |
    | db_name          | No           | Hive テーブルまたは Hudi テーブルが存在するデータベースの名前。 |
    | table_name       | Yes          | Hive テーブルまたは Hudi テーブルの名前。                    |
    | partition_name   | No           | Hive テーブルまたは Hudi テーブルのパーティションの名前。このパラメータを指定すると、StarRocks にキャッシュされた Hive テーブルおよび Hudi テーブルのパーティションのメタデータが更新されます。 |

## 使用上の注意

`ALTER_PRIV` 権限を持つユーザーのみが、このステートメントを実行して StarRocks にキャッシュされた Hive テーブルおよび Hudi テーブルのメタデータを更新できます。

## 例

異なるケースでの使用例は次のとおりです。

### 外部テーブル

例 1: 外部テーブル `hive1` を指定して、StarRocks における対応する Hive テーブルのキャッシュされたメタデータを更新します。

```SQL
REFRESH EXTERNAL TABLE hive1;
```

例 2: 外部テーブル `hudi1` と対応する Hudi テーブルのパーティションを指定して、StarRocks における対応する Hudi テーブルのキャッシュされたメタデータを更新します。

```SQL
REFRESH EXTERNAL TABLE hudi1
PARTITION ('date=2022-12-20', 'date=2022-12-21');
```

### 外部カタログ

例 1: StarRocks における `hive_table` のキャッシュされたメタデータを更新します。

```SQL
REFRESH EXTERNAL TABLE hive_catalog.hive_db.hive_table;
```

または

```SQL
USE hive_catalog.hive_db;
REFRESH EXTERNAL TABLE hive_table;
```

例 2: StarRocks における `hudi_table` のパーティションのキャッシュされたメタデータを更新します。

```SQL
REFRESH EXTERNAL TABLE hudi_catalog.hudi_db.hudi_table
PARTITION ('date=2022-12-20', 'date=2022-12-21');
```