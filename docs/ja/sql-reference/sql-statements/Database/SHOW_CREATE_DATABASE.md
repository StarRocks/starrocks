---
displayed_sidebar: docs
---

# SHOW CREATE DATABASE

データベースを作成するために使用された SQL コマンドを表示します。

## Syntax

```sql
SHOW CREATE DATABASE <db_name>
```

## Parameters

`db_name`: データベース名、必須。

## Returns

- `Database`: データベース名

- `Create Database`: データベースを作成するために使用された SQL コマンド

## Examples

```sql
mysql > show create database zj_test;
+----------+---------------------------+
| Database | Create Database           |
+----------+---------------------------+
| zj_test  | CREATE DATABASE `zj_test` |
+----------+---------------------------+
```

## References

- [ CREATE DATABASE](CREATE_DATABASE.md)
- [ SHOW DATABASES](SHOW_DATABASES.md)
- [ USE](USE.md)
- [ DESC](../table_bucket_part_index/DESCRIBE.md)
- [ DROP DATABASE](DROP_DATABASE.md)