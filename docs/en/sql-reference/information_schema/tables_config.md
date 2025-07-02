---
displayed_sidebar: docs
---

# tables_config

`tables_config` provides information about the configuration of tables.

The following fields are provided in `tables_config`:

| **Field**        | **Description**                                              |
| ---------------- | ------------------------------------------------------------ |
| TABLE_SCHEMA     | Name of the database that stores the table.                  |
| TABLE_NAME       | Name of the table.                                           |
| TABLE_ENGINE     | Engine type of the table.                                    |
| TABLE_MODEL      | Data model of the table. Valid values: `DUP_KEYS`, `AGG_KEYS`, `UNQ_KEYS` or `PRI_KEYS`. |
| PRIMARY_KEY      | Primary key of a Primary Key table or a Unique Key table. An empty string is returned if the table is not a Primary Key table or a Unique Key table. |
| PARTITION_KEY    | Partitioning columns of the table.                       |
| DISTRIBUTE_KEY   | Bucketing columns of the table.                          |
| DISTRIBUTE_TYPE  | Data distribution method of the table.                   |
| DISTRUBTE_BUCKET | Number of buckets in the table.                              |
| SORT_KEY         | Sort keys of the table.                                      |
| PROPERTIES       | Properties of the table.                                     |
| TABLE_ID         | ID of the table.                                             |
