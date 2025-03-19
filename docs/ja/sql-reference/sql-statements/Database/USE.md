---
displayed_sidebar: docs
---

# USE

## 説明

セッションのアクティブなデータベースを指定します。その後、テーブルの作成やクエリの実行などの操作を行うことができます。

## 構文

```SQL
USE [<catalog_name>.]<db_name>
```

## パラメータ

| **パラメータ** | **必須** | **説明**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| catalog_name  | いいえ           | catalog 名。<ul><li>このパラメータが指定されていない場合、`default_catalog` のデータベースがデフォルトで使用されます。</li><li>external catalog のデータベースを使用する場合、このパラメータを指定する必要があります。詳細は、例 2 を参照してください。</li><li>異なる catalog 間でデータベースを切り替える場合、このパラメータを指定する必要があります。詳細は、例 3 を参照してください。</li></ul>catalog についての詳細は、[概要](../../../data_source/catalog/catalog_overview.md)を参照してください。 |
| db_name       | はい          | データベース名。データベースは存在している必要があります。                  |

## 例

例 1: `default_catalog` の `example_db` をセッションのアクティブなデータベースとして使用します。

```SQL
USE default_catalog.example_db;
```

または

```SQL
USE example_db;
```

例 2: `hive_catalog` の `example_db` をセッションのアクティブなデータベースとして使用します。

```SQL
USE hive_catalog.example_db;
```

例 3: セッションのアクティブなデータベースを `hive_catalog.example_table1` から `iceberg_catalog.example_table2` に切り替えます。

```SQL
USE iceberg_catalog.example_table2;
```