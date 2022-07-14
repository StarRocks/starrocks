# System Restrictions

This section describes rules and restrictions of  using the StarRocks system.

1. StarRocks uses the MySQL protocol for communication, and users can connect to the StarRocks cluster via MySQL Client or JDBC. When choosing MySQL Client, it is recommended to use a version after 5.1. Versions before 5.1 don’t support usernames longer than 16 characters.
2. Naming conventions for cluster, database, table, column, partition, username, and role:
    * These names can only consist of numbers (0-9), letters (a-z or A-Z), and underscores (\_).
    * The length of the name cannot exceed 64 characters.
    * Except for column names, names for others should start with lowercase or uppercase letters only.
    * Column names can begin with underscore.
    * Database names and table names are case-sensitive, and column names are not case-sensitive.
3. Naming conventions for labels
The Label of the job can be specified when importing data. The label name can consist of hyphens (-), underscores (\_), letters (a-z or A-Z), and numbers (0-9), and cannot exceed 128 characters in length. The table signature has no requirement for starting characters.
4. When creating a table, the key column cannot be of float or double type, but the decimal type can be used to represent decimals.
5. Varchar can be up to 65533 bytes (two bytes less than 65535 because the first two bytes are used to indicate the length).
6. StarRocks only supports UTF8 encoding, but not GBK.
7. StarRocks does not support modifying column names in tables.
8. The default maximum length of SQL is 10000 bytes, which can be changed by `expr\_child\_limit` in `fe.conf`.
