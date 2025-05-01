---
displayed_sidebar: docs
---

# Default catalog

このトピックでは、default catalog が何であるか、そして default catalog を使用して StarRocks の内部データをどのようにクエリするかについて説明します。

StarRocks 2.3 以降では、StarRocks の内部データを管理するための内部 catalog を提供しています。各 StarRocks クラスターには、`default_catalog` という名前の内部 catalog が1つだけあります。現在、内部 catalog の名前を変更したり、新しい内部 catalog を作成したりすることはできません。

## 内部データのクエリ

1. StarRocks クラスターに接続します。
   - MySQL クライアントを使用して StarRocks クラスターに接続する場合、接続後にデフォルトで `default_catalog` に移動します。
   - JDBC を使用して StarRocks クラスターに接続する場合、接続時に `default_catalog.db_name` を指定することで、default catalog の目的のデータベースに直接移動できます。

2. (オプション) 次のステートメントを実行して、StarRocks 内のすべてのデータベースを表示します。このステートメントの出力を表示するには、[SHOW DATABASES](../../sql-reference/sql-statements/data-manipulation/SHOW_DATABASES.md) を参照してください。

      ```SQL
      SHOW DATABASES;
      ```

      または

      ```SQL
      SHOW DATABASES FROM catalog_name;
      ```

3. (オプション) 次のステートメントを実行して、目的のデータベースに移動します。

      ```SQL
      USE db_name;
      ```

      または

      ```SQL
      USE default_catalog.db_name;
      ```

4. 内部データをクエリします。SELECT ステートメントの詳細な使用法については、[SELECT](../../sql-reference/sql-statements/data-manipulation/SELECT.md) を参照してください。

      ```SQL
      SELECT * FROM table_name;
      ```

      前のステップでデータベースを指定しなかった場合、select クエリで直接指定できます。

      ```SQL
      SELECT * FROM db_name.table_name;
      ```

      または

      ```SQL
      SELECT * FROM default_catalog.db_name.table_name;
      ```

## 例

`olap_db.olap_table` のデータをクエリするには、次の操作のいずれかを実行できます。

```SQL
USE olap_db;
SELECT * FROM olap_table limit 1;
```

または

```SQL
SELECT * FROM olap_db.olap_table limit 1;     
```

または

```SQL
SELECT * FROM default_catalog.olap_db.olap_table limit 1;      
```

## 参考文献

外部データソースからデータをクエリするには、[Query external data](../catalog/query_external_data.md) を参照してください。