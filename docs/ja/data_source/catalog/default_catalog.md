---
displayed_sidebar: docs
---

# Default catalog

このトピックでは、default catalog とは何か、そして default catalog を使用して StarRocks の内部データをどのようにクエリするかについて説明します。

StarRocks 2.3以降では、StarRocks の内部データを管理するための internal catalog を提供しています。各 StarRocks クラスターには `default_catalog` という名前の internal catalog が1つだけあります。現在、internal catalog の名前を変更したり、新しい internal catalog を作成したりすることはできません。

## 内部データのクエリ

1. StarRocks クラスターに接続します。
   - MySQL クライアントを使用して StarRocks クラスターに接続する場合、接続後にデフォルトで `default_catalog` に移動します。
   - JDBC を使用して StarRocks クラスターに接続する場合、接続時に `default_catalog.db_name` を指定することで、default catalog の目的のデータベースに直接移動できます。

2. （オプション）[SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用してデータベースを表示します:

      ```SQL
      SHOW DATABASES;
      ```

      または

      ```SQL
      SHOW DATABASES FROM <catalog_name>;
      ```

3. （オプション）[SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用して、現在のセッションで目的の catalog に切り替えます:

    ```SQL
    SET CATALOG <catalog_name>;
    ```

    その後、[USE](../../sql-reference/sql-statements/Database/USE.md) を使用して、現在のセッションでアクティブなデータベースを指定します:

    ```SQL
    USE <db_name>;
    ```

    または、[USE](../../sql-reference/sql-statements/Database/USE.md) を使用して、目的の catalog のアクティブなデータベースに直接移動することもできます:

    ```SQL
    USE <catalog_name>.<db_name>;
    ```

4. [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を使用して内部データをクエリします:

      ```SQL
      SELECT * FROM <table_name>;
      ```

      前のステップでアクティブなデータベースを指定しなかった場合、select クエリで直接指定できます:

      ```SQL
      SELECT * FROM <db_name>.<table_name>;
      ```

      または

      ```SQL
      SELECT * FROM default_catalog.<db_name>.<table_name>;
      ```

## 例

`olap_db.olap_table` のデータをクエリするには、次のいずれかの操作を行います:

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