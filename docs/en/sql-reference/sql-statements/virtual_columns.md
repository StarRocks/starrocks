---
displayed_sidebar: docs
---

# Virtual columns

StarRocks supports virtual columns, which are read-only metadata columns that are automatically available for querying but are not stored in the table schema. Virtual columns provide metadata information about the data storage structure and can be used for data distribution analysis and troubleshooting.

Currently, StarRocks supports the following virtual column:

- `_tablet_id_`: Returns the tablet ID (BIGINT) for each row, indicating which tablet the row belongs to.

## Features

- **Hidden by default**: Virtual columns do not appear in `SELECT *`, `DESCRIBE`, or `SHOW CREATE TABLE` statements.
- **Read-only**: Virtual columns are automatically computed and cannot be modified or inserted.
- **Query support**: Virtual columns can be used in SELECT, WHERE, GROUP BY, ORDER BY, JOIN, and aggregation functions.

## Syntax

To use a virtual column in a query, explicitly specify it in the SELECT clause:

```SQL
SELECT _tablet_id_, <other_columns> FROM <table_name>;
```

## Common use cases

### 1. Data distribution analysis

Analyze how data is distributed across tablets to identify data skew:

```SQL
SELECT _tablet_id_,
       COUNT(*) AS row_count,
       SUM(k2) AS total_value
FROM test_table
GROUP BY _tablet_id_
ORDER BY row_count DESC;
```

### 2. Tablet-level statistics

Get statistics for each tablet to understand data distribution:

```SQL
SELECT _tablet_id_,
       COUNT(*) AS rows,
       MIN(k1) AS min_key,
       MAX(k1) AS max_key,
       AVG(k2) AS avg_value
FROM test_table
GROUP BY _tablet_id_;
```

### 3. Troubleshooting

Identify which tablet contains specific data for troubleshooting:

```SQL
-- Find tablets containing specific values
SELECT _tablet_id_, k1, k2
FROM test_table
WHERE k1 = 100;
```

## Limitations

- Virtual columns are **not included** in `SELECT *` statements. You must explicitly specify them in the SELECT clause.
- Virtual columns **do not appear** in `DESCRIBE` or `SHOW CREATE TABLE` statements.
- Virtual columns **cannot be modified** or inserted directly.
- Virtual columns are **read-only** and computed at query time.

## Notes

- The `_tablet_id_` value is a BIGINT that uniquely identifies the tablet containing the row.
- Virtual columns are computed at query execution time and do not consume storage space.
- Virtual columns can be used with all standard SQL operations including WHERE, GROUP BY, ORDER BY, JOIN, and aggregation functions.
