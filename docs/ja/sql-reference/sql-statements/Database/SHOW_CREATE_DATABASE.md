---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Shows the SQL command used to create a database."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# SHOW CREATE DATABASE

データベースを作成するために使用された SQL コマンドを表示します。

## 構文

```sql
SHOW CREATE DATABASE <db_name>
```

## パラメータ

`db_name`: データベース名、必須。

## 戻り値

- `Database`: データベース名

- `Create Database`: データベースを作成するために使用された SQL コマンド

## 例

```sql
mysql > show create database zj_test;
+----------+---------------------------+
| Database | Create Database           |
+----------+---------------------------+
| zj_test  | CREATE DATABASE `zj_test` |
+----------+---------------------------+
```

## 参考

- [CREATE DATABASE](CREATE_DATABASE.md)
- [SHOW DATABASES](SHOW_DATABASES.md)
- [USE](USE.md)
- [DESC](../table_bucket_part_index/DESCRIBE.md)
- [DROP DATABASE](DROP_DATABASE.md)