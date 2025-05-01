---
displayed_sidebar: docs
---

# USE

## 説明

セッションで使用するアクティブなデータベースを指定します。これにより、テーブルの作成やクエリの実行などの操作を行うことができます。

## 構文

```SQL
USE [<catalog_name>.]<db_name>
```

## パラメータ

| **パラメータ** | **必須**    | **説明**                                                      |
| ------------- | ------------ | ------------------------------------------------------------ |
| catalog_name  | いいえ       | カタログ名。<ul><li>このパラメータが指定されていない場合、`default_catalog` のデータベースがデフォルトで使用されます。</li><li>外部 catalog のデータベースを使用する場合、このパラメータを指定する必要があります。詳細は、例2を参照してください。</li><li>異なる catalogs 間でデータベースを切り替える場合、このパラメータを指定する必要があります。詳細は、例3を参照してください。</li></ul>catalogs についての詳細は、 [概要](../../../data_source/catalog/catalog_overview.md) を参照してください。 |
| db_name       | はい         | データベース名。データベースは存在している必要があります。                  |

## 例

例1: `default_catalog` から `example_db` をセッションのアクティブなデータベースとして使用します。

```SQL
USE default_catalog.example_db;
```

または

```SQL
USE example_db;
```

例2: `hive_catalog` から `example_db` をセッションのアクティブなデータベースとして使用します。

```SQL
USE hive_catalog.example_db;
```

例3: セッションのアクティブなデータベースを `hive_catalog.example_table1` から `iceberg_catalog.example_table2` に切り替えます。

```SQL
USE iceberg_catalog.example_table2;
```