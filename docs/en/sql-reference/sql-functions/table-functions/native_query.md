---
displayed_sidebar: docs
toc_max_heading_level: 4
---

# `native_query`

`native_query()` is a JDBC query table function. It sends a `SELECT` statement directly to a JDBC catalog, infers the result schema from the remote database, and exposes the result as a regular StarRocks table relation.

Use this function when a query against a JDBC source cannot be expressed as a single external table scan, for example, when the query needs remote joins, remote views, vendor-specific predicates, or a pre-filtered subquery.

## Syntax

```SQL
TABLE(<catalog_name>.native_query('<sql>'))
```

## Parameters

- `catalog_name`: The name of an existing JDBC catalog.
- `sql`: A string literal that contains one remote `SELECT` query. If the remote query itself contains single quotes, escape them by doubling them, for example, `''2026-01-01''`.

## Return value

Returns a table whose schema is inferred from the result metadata of the remote query.

After `native_query()` returns the remote result set, StarRocks can continue to apply local filters, joins, projections, aggregations, and `INSERT INTO` processing on top of that result.

## Usage notes

- `native_query()` is supported only for JDBC catalogs.
- You must call `native_query()` with `TABLE()`.
- `native_query()` accepts exactly one argument, and the argument must be a string literal.
- Named arguments are not supported.
- The pass-through SQL must start with `SELECT`. `WITH` queries and non-`SELECT` statements are not supported.
- A table alias is supported, but a column alias list after the table function is not supported. If you need specific output column names, define the aliases inside the pass-through SQL itself.
- The output columns should have distinct names. If the remote query returns duplicate column names, assign unique aliases in the pass-through SQL.
- The current user must have the `USAGE` privilege on the target JDBC catalog.

## Examples

Example 1: Query a remote table through a JDBC catalog.

```SQL
SELECT *
FROM TABLE(pg_jdbc.native_query(
    'SELECT c_custkey, c_name, c_nationkey FROM customer'
));
```

Example 2: Use database-native filtering in the pass-through SQL, and then apply additional filtering in StarRocks.

```SQL
SELECT *
FROM TABLE(pg_jdbc.native_query(
    'SELECT id, email FROM app_user WHERE email ILIKE ''%starrocks.com'''
)) AS t
WHERE t.id < 100;
```

Example 3: Join the remote query result with a local StarRocks table.

```SQL
SELECT local_ids.id, remote_users.name
FROM local_ids
JOIN TABLE(pg_jdbc.native_query(
    'SELECT id, name FROM dim_user'
)) AS remote_users
ON local_ids.id = remote_users.id;
```

Example 4: Load the remote query result into an internal StarRocks table.

```SQL
INSERT INTO sr_orders_stage
SELECT order_id, customer_id, amount
FROM TABLE(pg_jdbc.native_query(
    'SELECT order_id, customer_id, amount
     FROM orders
     WHERE order_date >= DATE ''2026-01-01'''
));
```

## Related topics

- [JDBC catalog](../../../data_source/catalog/jdbc_catalog.md)

## keywords

table function, jdbc, native query, jdbc catalog
