---
displayed_sidebar: docs
---

# partitions

:::note

This view does not apply to the available features in StarRocks.

:::

`partitions` provides information about table partitions.

The following fields are provided in `partitions`:

| **Field**                  | **Description**                                              |
| -------------------------- | ------------------------------------------------------------ |
| TABLE_CATALOG              | The name of the catalog to which the table belongs. This value is always `def`. |
| TABLE_SCHEMA               | The name of the database to which the table belongs.         |
| TABLE_NAME                 | The name of the table containing the partition.              |
| PARTITION_NAME             | The name of the partition.                                   |
| SUBPARTITION_NAME          | If the `PARTITIONS` table row represents a subpartition, the name of subpartition; otherwise `NULL`.For `NDB`: This value is always `NULL`. |
| PARTITION_ORDINAL_POSITION | All partitions are indexed in the same order as they are defined, with `1` being the number assigned to the first partition. The indexing can change as partitions are added, dropped, and reorganized; the number shown is this column reflects the current order, taking into account any indexing changes. |
| PARTITION_METHOD           | Valid values: `RANGE`, `LIST`, `HASH`, `LINEAR HASH`, `KEY`, or `LINEAR KEY`. |
| SUBPARTITION_METHOD        | Valid values: `HASH`, `LINEAR HASH`, `KEY`, or `LINEAR KEY`  |
| PARTITION_EXPRESSION       | The expression for the partitioning function used in the `CREATE TABLE` or `ALTER TABLE` statement that created the table's current partitioning scheme. |
| SUBPARTITION_EXPRESSION    | This works in the same fashion for the subpartitioning expression that defines the subpartitioning for a table as `PARTITION_EXPRESSION` does for the partitioning expression used to define a table's partitioning. If the table has no subpartitions, this column is `NULL`. |
| PARTITION_DESCRIPTION      | This column is used for RANGE and LIST partitions. For a `RANGE` partition, it contains the value set in the partition's `VALUES LESS THAN` clause, which can be either an integer or `MAXVALUE`. For a `LIST` partition, this column contains the values defined in the partition's `VALUES IN` clause, which is a list of comma-separated integer values.For partitions whose `PARTITION_METHOD` is other than `RANGE` or `LIST`, this column is always `NULL`. |
| TABLE_ROWS                 | The number of table rows in the partition.                   |
| AVG_ROW_LENGTH             | The average length of the rows stored in this partition or subpartition, in bytes. This is the same as `DATA_LENGTH` divided by `TABLE_ROWS`. |
| DATA_LENGTH                | The total length of all rows stored in this partition or subpartition, in bytes; that is, the total number of bytes stored in the partition or subpartition. |
| MAX_DATA_LENGTH            | The maximum number of bytes that can be stored in this partition or subpartition. |
| INDEX_LENGTH               | The length of the index file for this partition or subpartition, in bytes. |
| DATA_FREE                  | The number of bytes allocated to the partition or subpartition but not used. |
| CREATE_TIME                | The time that the partition or subpartition was created.     |
| UPDATE_TIME                | The time that the partition or subpartition was last modified. |
| CHECK_TIME                 | The last time that the table to which this partition or subpartition belongs was checked. |
| CHECKSUM                   | The checksum value, if any; otherwise `NULL`.                |
| PARTITION_COMMENT          | The text of the comment, if the partition has one. If not, this value is empty. |
| NODEGROUP                  | This is the nodegroup to which the partition belongs.        |
| TABLESPACE_NAME            | The name of the tablespace to which the partition belongs.   |
