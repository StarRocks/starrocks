---
displayed_sidebar: docs
---

# native_query

`native_query` is a JDBC catalog table function. It executes a database-native `SELECT` statement through a JDBC catalog and exposes the result as a StarRocks relation.

Use this function when the source database must run SQL that is hard to express against a single JDBC external table, such as a pre-filtered subquery, a join in the source database, or vendor-specific SQL syntax. After `native_query` returns a relation, you can use StarRocks to apply additional filters, joins, aggregations, projections, or load the result with [INSERT INTO](../../sql-statements/loading_unloading/INSERT.md).

This function is supported from v4.1 onwards.

## Syntax

```SQL
SELECT ...
FROM TABLE(<jdbc_catalog>.native_query('<select_sql>')) [AS] <alias>
[WHERE ...];
```

## Parameters

### `jdbc_catalog`

The name of a [JDBC catalog](../../../data_source/catalog/jdbc_catalog.md). Only JDBC catalogs support this function.

### `select_sql`

A string literal that contains the pass-through SQL statement executed by the source database.

After StarRocks removes leading comments and trailing semicolons from `select_sql`, the statement must start with `SELECT`. Use the SQL dialect, object names, quoting rules, and functions of the source database.

If `select_sql` contains a single quotation mark (`'`), escape it as two single quotation marks (`''`) in the StarRocks SQL string.

### `alias`

An optional table alias for the result relation.

Column aliases after the table alias, such as `AS q(c1, c2)`, are not supported. Define column aliases inside `select_sql`, for example `SELECT id AS id_alias FROM ...`.

## Return value

`native_query` returns a relation whose columns are inferred from the JDBC result set metadata of `select_sql`. StarRocks maps JDBC column types to StarRocks column types by using the schema resolver of the JDBC catalog.

## Usage notes

- The StarRocks user needs the `USAGE` privilege on the JDBC catalog. Permissions on source database objects are checked by the remote database with the user configured in the JDBC catalog.
- `select_sql` must be a string literal and must be the only argument. Named arguments are not supported.
- `select_sql` must start with `SELECT` after leading comments are removed. Statements that start with `WITH`, `INSERT`, `UPDATE`, `DELETE`, or other non-`SELECT` keywords are not supported.
- The legacy `<catalog>.system.query(...)` form is not supported.
- If the source query returns no columns, StarRocks returns an error during analysis.

## Examples

The following examples assume that the source MySQL database contains a table named `app.orders` and that a JDBC catalog named `jdbc0` has been created in StarRocks.

Example 1: Run a source-side subquery and apply an outer StarRocks filter.

```SQL
SELECT id, name, doubled_score
FROM TABLE(jdbc0.native_query(
    'SELECT id, name, score * 2 AS doubled_score
     FROM app.orders
     WHERE score >= 20'
)) q
WHERE doubled_score < 70
ORDER BY id;
```

Example 2: Aggregate the result returned by the pass-through query.

```SQL
SELECT category, SUM(score) AS total_score
FROM TABLE(jdbc0.native_query(
    'SELECT category, score
     FROM app.orders
     WHERE status = ''PAID'''
)) q
GROUP BY category
ORDER BY category;
```

Example 3: Load the result of a native query into a StarRocks table.

```SQL
INSERT INTO paid_order_summary
SELECT category, SUM(score) AS total_score
FROM TABLE(jdbc0.native_query(
    'SELECT category, score
     FROM app.orders
     WHERE status = ''PAID'''
)) q
GROUP BY category;
```

## Unsupported forms

```SQL
-- Named arguments are not supported.
SELECT * FROM TABLE(jdbc0.native_query(query => 'SELECT id FROM app.orders'));

-- The legacy system.query alias is not supported.
SELECT * FROM TABLE(jdbc0.system.query('SELECT id FROM app.orders'));

-- WITH queries are not supported because the SQL must start with SELECT.
SELECT * FROM TABLE(jdbc0.native_query('WITH q AS (SELECT id FROM app.orders) SELECT * FROM q'));

-- Column aliases after the table alias are not supported.
SELECT * FROM TABLE(jdbc0.native_query('SELECT id FROM app.orders')) q(id_alias);
```
