---
displayed_sidebar: docs
---

# SHOW FILE

You can execute the SHOW FILE statement to view the information about a file stored in a database.

:::tip

Users with any privilege on the database to which a file belongs can perform this operation. If a file belongs to a database, all users that have access to this database can use this file.

:::

## Syntax

```SQL
SHOW FILE [FROM database]
```

The file information returned by this statement is as follows:

- `Id`: the globally unique ID of the file.

- `DbName`: the database to which the file belongs.

- `GlobalStateMgr`: the category to which the file belongs (This corresponds to the `catalog` property specified during file creation).

- `FileName`: the name of the file.

- `FileSize`: the size of the file. The unit is bytes.

- `IsContent`: indicates whether the file content has been fully uploaded and stored (true or false).

- `MD5`: the message-digest algorithm that is used to check the file.

## Examples

View the files stored in `test_db`.

```Plain
mysql> SHOW FILE FROM test_db;

+-------+---------+----------------+---------------+----------+-----------+----------------------------------+
| Id    | DbName  | GlobalStateMgr | FileName      | FileSize | IsContent | MD5                              |
+-------+---------+----------------+---------------+----------+-----------+----------------------------------+
| 24016 | test_db | kafka          | my_secret.txt | 43       | true      | a7c71293869ec817515e61a9f0cfb48e |
| 24072 | test_db | configs        | db_config.txt | 18       | true      | b1a32aaac8a739eac7b55a4364df8876 |
+-------+---------+----------------+---------------+----------+-----------+----------------------------------+
2 rows in set (0.01 sec)
```
