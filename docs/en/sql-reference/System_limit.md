---
displayed_sidebar: docs
description: "System limits and naming rules for StarRocks objects including databases, tables, partitions, columns, and identifiers."
---

# System limits

This topic describes the rules and limits that apply when you use StarRocks.

- StarRocks uses the MySQL protocol for communication. You can connect to the StarRocks cluster via a MySQL client or JDBC. We recommend that you use a MySQL client whose version is 5.1 or later. Versions earlier than 5.1 do not support usernames that are longer than 16 characters.

- Naming conventions for objects such as catalogs, databases, tables, views, asynchronous materialized views, partitions, columns, indexes, usernames, roles, repositories, resources, storage volumes, and pipes:

  - The name can only consist of digits (0-9), letters (a-z or A-Z), and underscores (`_`). **Usernames can be all digits.**
  - The name can start with a letter or an underscore (`_`).
  - The name cannot exceed 64 characters in length, **among which:**
    - Database name cannot exceed 256 characters.
    - Table name and column name cannot exceed 1024 characters.
    - Username cannot exceed 128 characters.
  - Column name (column alias), partition name, and index name are **not** case-sensitive. Other names are **case-sensitive**.

- The FE configuration item `enable_table_name_case_insensitive` (supported from v4.0 onwards) controls whether catalog names, database names, table names, view names, and asynchronous materialized view names are case-insensitive. This feature is **disabled by default** (these names are case-sensitive), and it can be enabled only when creating a cluster.

  :::warning

  **We strongly recommend that you keep this feature disabled unless you have a specific, well-understood reason to enable it.**

  When enabled, StarRocks stores the affected names in lowercase and forcibly converts every catalog, database, table, view, and materialized view name to lowercase during **both query and write (DDL/DML) processing**. This has the following consequences:

  - **It can make external tables and external catalogs unusable.** Different external catalog services follow different naming and case-sensitivity conventions. If an external schema, database, or table name is not already in lowercase, StarRocks lowercases the name in your SQL before passing it to the connector and then looks up a name that does not exist in the source, so the query fails with a "not found" error.
  - **It cannot be changed after the cluster is created.** After the cluster is started, the value of this configuration cannot be modified by any means. Any attempt to modify it results in an error, and FE fails to start when it detects that the value is inconsistent with the value used when the cluster was first started.

  Only enable this feature on a new cluster where you are certain that **all** object names — including those in every external data source you plan to access — are already in lowercase.

  :::

- Naming conventions for labels:
  You can specify the label of a job when you load data. The label name can consist of digits (0-9), letters (a-z or A-Z), and underscores (`_`), and cannot exceed 128 characters in length. Label names can start with a letter or an underscore (`_`).

- When you create a table, the key column cannot be of the FLOAT or DOUBLE type. You can use the DECIMAL type to represent decimals.

- The maximum length of a VARCHAR value varies in different versions:

  - In versions earlier than StarRocks 2.1, the length ranges from 1 to 65533 bytes.
  - [Preview] In StarRocks 2.1 and later versions, the length ranges from 1 to 1048576 bytes. Maximum length of a VARCHAR value = Maximum row size (1048578 bytes) - Length prefix (2 bytes). The length prefix indicates the number of bytes in the value.
  - The default length is 1 byte.

- StarRocks supports only UTF-8 encoding, not GBK.

- StarRocks does not support modifying the table types of an existing table. For example, you cannot change a Duplicate Key table to a Primary Key table. You must create a new table.

- By default, a query can be nested with a maximum of 10,000 subqueries, which is controlled by the FE parameter `expr_children_limit`.
