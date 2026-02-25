---
displayed_sidebar: docs
---

# 外部データのクエリ

このトピックでは、external catalog を使用して外部データソースからデータをクエリする方法を案内します。

## 前提条件

external catalog は外部データソースに基づいて作成されます。サポートされている external catalog の種類については、[Catalog](../catalog/catalog_overview.md#catalog) を参照してください。

## 手順

1. StarRocks クラスターに接続します。
   - MySQL クライアントを使用して StarRocks クラスターに接続する場合、接続後はデフォルトで `default_catalog` に移動します。
   - JDBC を使用して StarRocks クラスターに接続する場合、接続時に `default_catalog.db_name` を指定することで、デフォルトカタログ内の目的のデータベースに直接移動できます。

2. (オプション) 次のステートメントを実行して、すべての catalog を表示し、作成した external catalog を見つけます。このステートメントの出力を確認するには、[SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を参照してください。

      ```SQL
      SHOW CATALOGS;
      ```

3. (オプション) 次のステートメントを実行して、external catalog 内のすべてのデータベースを表示します。このステートメントの出力を確認するには、[SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を参照してください。

      ```SQL
      SHOW DATABASES FROM catalog_name;
      ```

4. (オプション) 次のステートメントを実行して、external catalog 内の目的のデータベースに移動します。

      ```SQL
      USE catalog_name.db_name;
      ```

5. 外部データをクエリします。SELECT ステートメントの詳細な使用方法については、[SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を参照してください。

      ```SQL
      SELECT * FROM table_name;
      ```

      前の手順で external catalog とデータベースを指定しなかった場合、select クエリで直接指定できます。

      ```SQL
      SELECT * FROM catalog_name.db_name.table_name;
      ```

## 例

すでに `hive1` という名前の Hive catalog を作成し、Apache Hive™ クラスターの `hive_db.hive_table` からデータをクエリするために `hive1` を使用したい場合、次のいずれかの操作を行うことができます。

```SQL
USE hive1.hive_db;
SELECT * FROM hive_table limit 1;
```

または

```SQL
SELECT * FROM hive1.hive_db.hive_table limit 1;
```

## 参考文献

StarRocks クラスターからデータをクエリするには、[Default catalog](../catalog/default_catalog.md) を参照してください。