---
displayed_sidebar: docs
---

# SHOW TABLES

## 説明

StarRocks データベースまたは外部データソースのデータベース内のすべてのテーブルを表示します。例えば、Hive、Iceberg、Hudi、Delta Lake などです。

> **注意**
>
> 外部データソースのテーブルを表示するには、そのデータソースに対応する external catalog に対する USAGE 権限が必要です。

## 構文

```sql
SHOW TABLES [FROM <catalog_name>.<db_name>]
```

## パラメータ

| **パラメータ**          | **必須** | **説明**                                                     |
| ----------------- | -------- | ------------------------------------------------------------ |
| catalog_name | いいえ       | 内部 catalog または external catalog の名前。<ul><li>このパラメータを指定しないか、`default_catalog` に設定した場合、StarRocks データベースのテーブルが返されます。</li><li>このパラメータを external catalog の名前に設定した場合、外部データソースのデータベースのテーブルが返されます。</li></ul> 内部および外部の catalogs を表示するには、[SHOW CATALOGS](../Catalog/SHOW_CATALOGS.md) を実行できます。|
| db_name | いいえ       | データベース名。指定しない場合、デフォルトで現在のデータベースが使用されます。 |

## 例

例 1: StarRocks クラスターに接続した後、`default_catalog` のデータベース `example_db` のテーブルを表示します。次の2つのステートメントは同等です。

```plain
show tables from example_db;
+----------------------------+
| Tables_in_example_db       |
+----------------------------+
| depts                      |
| depts_par                  |
| emps                       |
| emps2                      |
+----------------------------+

show tables from default_catalog.example_db;
+----------------------------+
| Tables_in_example_db       |
+----------------------------+
| depts                      |
| depts_par                  |
| emps                       |
| emps2                      |
+----------------------------+
```

例 2: データベース `example_db` に接続した後、現在のデータベースのテーブルを表示します。

```plain
show tables;
+----------------------------+
| Tables_in_example_db       |
+----------------------------+
| depts                      |
| depts_par                  |
| emps                       |
| emps2                      |
+----------------------------+
```

例 3: external catalog `hudi_catalog` のデータベース `hudi_db` のテーブルを表示します。

```plain
show tables from hudi_catalog.hudi_db;
+----------------------------+
| Tables_in_hudi_db          |
+----------------------------+
| hudi_sync_mor              |
| hudi_table1                |
+----------------------------+
```

または、SET CATALOG を実行して external catalog `hudi_catalog` に切り替え、その後 `SHOW TABLES FROM hudi_db;` を実行することもできます。

## 参考

- [SHOW CATALOGS](../Catalog/SHOW_CATALOGS.md): StarRocks クラスター内のすべての catalogs を表示します。
- [SHOW DATABASES](../Database/SHOW_DATABASES.md): 内部 catalog または external catalog 内のすべてのデータベースを表示します。
- [SET CATALOG](../Catalog/SET_CATALOG.md): catalogs 間を切り替えます。