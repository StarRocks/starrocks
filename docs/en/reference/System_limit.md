# System limits

This topic describes the rules and limits that apply when you use StarRocks.

- StarRocks uses the MySQL protocol for communication. You can connect to the StarRocks cluster via a MySQL client or JDBC. We recommend that you use a MySQL client whose version is 5.1 or later. Versions earlier than 5.1 do not support usernames that are longer than 16 characters.

- Naming conventions for clusters, databases, tables, views, columns, partitions, usernames, and roles:

  - The names can only consist of digits (0-9), letters (a-z or A-Z), and underscores (\_).
  - The name cannot exceed 64 characters in length.
  - Except for column names, other names must start with a lowercase or uppercase letter.
  - Column names can start with an underscore (\_).
  - Database, table, and view names are case-sensitive. Column names are not case-sensitive.

- Naming conventions for labels:
  You can specify the label of a job when you import data. The label name can consist of underscores (\_), letters (a-z or A-Z), and digits (0-9), and cannot exceed 128 characters in length. The label name has no requirement for the starting character.

- When you create a table, the key column cannot be of the FLOAT or DOUBLE type. You can use the DECIMAL type to represent decimals.

- A VARCHAR value can be up to 65533 bytes (two bytes less than 65535 because the first two bytes are used to indicate the length).

- StarRocks supports only UTF8 encoding, not GBK.

- StarRocks does not support modifying column names in tables.

- By default, an expression can have a maximum of 10,000 children, which can be modified by using `expr_child_limit` in the `fe.conf` file.
