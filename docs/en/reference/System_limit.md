---
displayed_sidebar: "English"
---

# System limits

This topic describes the rules and limits that apply when you use StarRocks.

- StarRocks uses the MySQL protocol for communication. You can connect to the StarRocks cluster via a MySQL client or JDBC. We recommend that you use a MySQL client whose version is 5.1 or later. Versions earlier than 5.1 do not support usernames that are longer than 16 characters.

- Naming conventions for objects such as catalogs, databases, tables, views, partitions, columns, usernames, and roles:

  - The name can only consist of digits (0-9), letters (a-z or A-Z), and underscores (\_). **Usernames can be all digits.**
  - The name can start with a letter or an understore (\_).
  - The name cannot exceed 64 characters in length, **among which:**
    - Database name cannot exceed 256 characters.
    - Table name and column name cannot exceed 1024 characters.
    - Username cannot exceed 128 characters.
  - Catalog, database, table, view, username, and role names are case-sensitive. Column and partition names are **not** case-sensitive.

- Naming conventions for labels:
  You can specify the label of a job when you import data. The label name can consist of underscores (\_), letters (a-z or A-Z), and digits (0-9), and cannot exceed 128 characters in length. The label name has no requirement for the starting character.

- When you create a table, the key column cannot be of the FLOAT or DOUBLE type. You can use the DECIMAL type to represent decimals.

- The maximum length of a VARCHAR value varies in different versions:

  - In versions earlier than StarRocks 2.1, the length ranges from 1 to 65533 bytes.
  - [Preview] In StarRocks 2.1 and later versions, the length ranges from 1 to 1048576 bytes. Maximum length of a VARCHAR value = Maximum row size (1048578 bytes) - Length prefix (2 bytes). The length prefix indicates the number of bytes in the value.

The default length is 1 byte.

- StarRocks supports only UTF-8 encoding, not GBK.

- StarRocks does not support modifying column names in tables.

- By default, a query can be nested with a maximum of 10,000 subqueries.
