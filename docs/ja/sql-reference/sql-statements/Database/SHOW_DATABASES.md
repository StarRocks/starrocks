---
displayed_sidebar: docs
---

# SHOW DATABASES

## 説明

現在の StarRocks クラスターまたは外部データソース内のデータベースを表示します。StarRocks は v2.3 以降、外部データソースのデータベースの表示をサポートしています。

## 構文

```SQL
SHOW DATABASES [FROM <catalog_name>]
```

## パラメータ

| **パラメータ**   | **必須** | **説明**                                                      |
| ---------------- | -------- | ------------------------------------------------------------ |
| catalog_name     | いいえ   | 内部 catalog または外部 catalog の名前。<ul><li>パラメータを指定しない場合、または内部 catalog の名前である `default_catalog` を指定した場合、現在の StarRocks クラスター内のデータベースを表示できます。</li><li>パラメータの値を外部 catalog の名前に設定した場合、対応する外部データソース内のデータベースを表示できます。[SHOW CATALOGS](../Catalog/SHOW_CATALOGS.md) を実行して、内部および外部の catalog を表示できます。</li></ul> |

## 例

例 1: 現在の StarRocks クラスター内のデータベースを表示します。

```SQL
SHOW DATABASES;
```

または

```SQL
SHOW DATABASES FROM default_catalog;
```

上記のステートメントの出力は以下の通りです。

```SQL
+----------+
| Database |
+----------+
| db1      |
| db2      |
| db3      |
+----------+
```

例 2: `Hive1` 外部 catalog を使用して Hive クラスター内のデータベースを表示します。

```SQL
SHOW DATABASES FROM hive1;

+-----------+
| Database  |
+-----------+
| hive_db1  |
| hive_db2  |
| hive_db3  |
+-----------+
```

## 参考

- [CREATE DATABASE](CREATE_DATABASE.md)
- [SHOW CREATE DATABASE](SHOW_CREATE_DATABASE.md)
- [USE](USE.md)
- [DESC](../table_bucket_part_index/DESCRIBE.md)
- [DROP DATABASE](DROP_DATABASE.md)