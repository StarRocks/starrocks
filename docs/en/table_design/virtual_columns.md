---
displayed_sidebar: docs
sidebar_position: 70
description: "How to use StarRocks virtual columns such as _tablet_id_, _segment_id_, and _row_id_ to inspect where each row of a native table is physically stored."
keywords: ['virtual column', 'tablet_id', 'row_id', 'segment_id', 'rowset_id']
---

# Virtual columns

Virtual columns are read-only metadata columns that StarRocks exposes on native tables. They describe **where a row is physically stored** — which tablet, rowset, segment, and row position it comes from.

Virtual columns are computed at query time on the BE (or CN) node. They are not stored on disk, consume no storage space, and are not part of the table schema. As a result, they:

- do not appear in `SELECT *`,
- do not appear in `DESCRIBE <table>` or `SHOW CREATE TABLE <table>`,
- must be referenced explicitly by name.

Virtual columns are mainly used for diagnostics, for example analyzing how data is distributed across tablets, or locating the physical position of a specific row.

## Available virtual columns

| Column            | Data type | Description                                                                           |
| ----------------- | --------- | ------------------------------------------------------------------------------------- |
| `_tablet_id_`     | BIGINT    | ID of the tablet that contains the row.                                                |
| `_rowset_id_`     | STRING    | ID of the rowset that contains the row.                                                |
| `_segment_id_`    | BIGINT    | ID of the segment that contains the row.                                               |
| `_rss_id_`        | INT       | Rowset-segment ID of the row within the tablet, derived from the rowset ID and the segment ID. |
| `_dynamic_rssid_` | INT       | Dynamic rowset-segment ID of the row.                                                  |
| `_row_id_`        | BIGINT    | Zero-based ordinal of the row within its segment.                                      |
| `_source_id_`     | INT       | ID of the BE or CN node from which the row is read.                                    |

Column names are case-insensitive: `_tablet_id_` and `_TABLET_ID_` refer to the same virtual column.

Together, `_tablet_id_`, `_rowset_id_` (or `_rss_id_`), `_segment_id_`, and `_row_id_` identify the physical position of a row at the moment it is read.

## Usage

Virtual columns are available on native tables in the internal catalog (Duplicate Key, Aggregate, Unique Key, and Primary Key tables) and on materialized views. Tables in external catalogs do not expose virtual columns.

You can reference a virtual column anywhere an ordinary column is allowed in a query: in the `SELECT` list, `WHERE`, `GROUP BY`, `ORDER BY`, aggregate functions, and joins. When a query involves multiple tables, qualify the column with the table name or alias, for example `t._tablet_id_`.

The following examples use this table:

```sql
CREATE TABLE olap_table (
    k1 INT,
    k2 INT,
    v1 STRING
)
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 3;

INSERT INTO olap_table VALUES
(1, 10, 'a'),
(2, 20, 'b'),
(3, 30, 'c'),
(4, 40, 'd'),
(5, 50, 'e');
```

### Query a virtual column

```sql
SELECT _tablet_id_, k1, k2 FROM olap_table ORDER BY k1;
+------------+------+------+
| _tablet_id_| k1   | k2   |
+------------+------+------+
|    3619836 |    1 |   10 |
|    3619834 |    2 |   20 |
|    3619836 |    3 |   30 |
|    3619838 |    4 |   40 |
|    3619838 |    5 |   50 |
+------------+------+------+
```

Virtual columns are hidden from `SELECT *`:

```sql
SELECT * FROM olap_table ORDER BY k1;
+------+------+------+
| k1   | k2   | v1   |
+------+------+------+
|    1 |   10 | a    |
|    2 |   20 | b    |
|    3 |   30 | c    |
|    4 |   40 | d    |
|    5 |   50 | e    |
+------+------+------+
```

### Analyze data distribution across tablets

Group by `_tablet_id_` to see how many rows each tablet holds. This is a quick way to detect data skew caused by an unbalanced bucketing key.

```sql
SELECT _tablet_id_, COUNT(*) AS row_count, SUM(k2) AS sum_k2
FROM olap_table
GROUP BY _tablet_id_
ORDER BY row_count DESC;
+------------+-----------+--------+
| _tablet_id_| row_count | sum_k2 |
+------------+-----------+--------+
|    3619836 |         2 |     40 |
|    3619838 |         2 |     90 |
|    3619834 |         1 |     20 |
+------------+-----------+--------+
```

To count the tablets that actually contain data:

```sql
SELECT COUNT(DISTINCT _tablet_id_) FROM olap_table;
```

### Locate the physical position of a row

```sql
SELECT k1, _tablet_id_, _rowset_id_, _segment_id_, _row_id_
FROM olap_table
WHERE k1 = 1;
```

### Filter on a virtual column

```sql
SELECT k1, k2, _tablet_id_
FROM olap_table
WHERE _tablet_id_ = 3619836
ORDER BY k1;
```

:::note
A predicate on a virtual column is evaluated while the data is being scanned. It does not reduce the set of tablets that the query reads.
:::

## Usage notes

- Virtual columns describe physical storage, not business data. Their values can change when the underlying data is reorganized — for example by compaction, by a new load, by a schema change, or by replica migration. Do not store them as business keys, and do not rely on them to correlate rows across queries.
- Virtual columns are query-only. They cannot be loaded into, updated, or declared in `CREATE TABLE`.
- StarRocks does not collect column statistics for virtual columns.
- The names of virtual columns are reserved. By default, you cannot create a table column with any of these names, because a table column of the same name shadows the virtual column and makes both impossible to reference in a query. If you need such a column name, set the FE dynamic parameter [`allow_system_reserved_names`](../administration/configuration/FE_parameters/shared_lake_other.md#allow_system_reserved_names) to `TRUE`. See [CREATE TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) for details.

## Enable or disable virtual columns

Virtual columns are controlled by the FE dynamic parameter [`enable_virtual_columns`](../administration/configuration/FE_parameters/user_query_loading.md#enable_virtual_columns), which defaults to `true`.

To disable them for the whole cluster:

```sql
ADMIN SET FRONTEND CONFIG ("enable_virtual_columns" = "false");
```

When virtual columns are disabled, a query that references one fails with an error such as `Column '_tablet_id_' cannot be resolved`.
