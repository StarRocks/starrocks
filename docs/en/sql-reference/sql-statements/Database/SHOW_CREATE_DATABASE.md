---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Shows the SQL command used to create a database."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# SHOW CREATE DATABASE

Shows the SQL command used to create a database.

## Syntax

```sql
SHOW CREATE DATABASE <db_name>
```

## Parameters

`db_name`: the database name, required.

## Returns

- `Database`: the database name

- `Create Database`: the SQL command used to create the database

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

- [CREATE DATABASE](CREATE_DATABASE.md)
- [SHOW DATABASES](SHOW_DATABASES.md)
- [USE](USE.md)
- [DESC](../table_bucket_part_index/DESCRIBE.md)
- [DROP DATABASE](DROP_DATABASE.md)
