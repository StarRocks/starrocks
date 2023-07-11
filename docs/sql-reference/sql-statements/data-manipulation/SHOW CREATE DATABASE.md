# SHOW CREATE DATABASE

Shows the SQL command used to create a database.

## Syntax

```sql
SHOW CREATE DATABASE <db_name>
```

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

- [CREATE DATABASE](../data-definition/CREATE%20DATABASE.md)
- [SHOW DATABASES](SHOW%20DATABASES.md)
- [USE](../data-definition/USE.md)
- [DESC](../Utility/DESCRIBE.md)
- [DROP DATABASE](../data-definition/DROP%20DATABASE.md)
