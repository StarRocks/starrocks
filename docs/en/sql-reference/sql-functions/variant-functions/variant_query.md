---
displayed_sidebar: docs
---

# variant_query

Queries the value of an element that can be located by the path expression in a VARIANT object and returns a VARIANT value.

This function is used to navigate and extract sub-elements from VARIANT data read from Iceberg tables in Parquet format.

## Syntax

```Haskell
variant_query(variant_expr, path)
```

## Parameters

- `variant_expr`: the expression that represents the VARIANT object. This is typically a VARIANT column from an Iceberg table.

- `path`: the expression that represents the path to an element in the VARIANT object. The value of this parameter is a string. The path syntax is similar to JSON path:
  - `$` represents the root element
  - `.` is used to access object fields
  - `[index]` is used to access array elements (0-based indexing)
  - Field names with special characters can be quoted: `$."field.name"`

## Return value

Returns a VARIANT value.

If the element does not exist or the path is invalid, the function returns NULL.

## Examples

Example 1: Query the root element of a VARIANT value.

```SQL
SELECT variant_query(data, '$') AS root_data
FROM bluesky
LIMIT 1;
```

```plaintext
+-------------------------------------------------------------------------------------------+
| root_data                                                                                 |
+-------------------------------------------------------------------------------------------+
| {"commit":{"collection":"app.bsky.graph.follow","operation":"delete","rev":"3lcgs4mw...}  |
+-------------------------------------------------------------------------------------------+
```

Example 2: Query a nested field in a VARIANT object.

```SQL
SELECT variant_query(data, '$.commit') AS commit_info
FROM bluesky
LIMIT 1;
```

```plaintext
+--------------------------------------------------------------------------------+
| commit_info                                                                    |
+--------------------------------------------------------------------------------+
| {"collection":"app.bsky.graph.follow","operation":"delete","rev":"3lcgs4mw...} |
+--------------------------------------------------------------------------------+
```

Example 3: Check the type of nested fields using variant_typeof.

```SQL
SELECT
    variant_typeof(variant_query(data, '$.commit')) AS commit_type,
    variant_typeof(variant_query(data, '$.time_us')) AS time_type
FROM bluesky
LIMIT 1;
```

```plaintext
+-------------+-----------+
| commit_type | time_type |
+-------------+-----------+
| Object      | Int64     |
+-------------+-----------+
```

Example 4: Cast the result to a SQL type.

```SQL
SELECT CAST(variant_query(data, '$.commit') AS STRING) AS commit_info
FROM bluesky
LIMIT 1;
```

```plaintext
+-----------------------------------------------------------------------------------+
| commit_info                                                                       |
+-----------------------------------------------------------------------------------+
| {"cid":"bafyreia3k...","collection":"app.bsky.feed.repost","operation":"create"...} |
+-----------------------------------------------------------------------------------+
```

Example 5: Filter using variant_query results.

```SQL
SELECT COUNT(*) AS total
FROM bluesky
WHERE variant_query(data, '$.commit.record') IS NOT NULL;
```

```plaintext
+---------+
| total   |
+---------+
| 9500118 |
+---------+
```

## keyword

VARIANT_QUERY,VARIANT
