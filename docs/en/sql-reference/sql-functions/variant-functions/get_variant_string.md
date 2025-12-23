---
displayed_sidebar: docs
---

# get_variant_string

Extracts a string value from a VARIANT object at the specified path.

This function navigates to the specified path in a VARIANT value and returns the value as a VARCHAR. If the value at the path is not a string or cannot be converted to a string, the function returns NULL.

## Syntax

```Haskell
VARCHAR get_variant_string(variant_expr, path)
```

## Parameters

- `variant_expr`: the expression that represents the VARIANT object. This is typically a VARIANT column from an Iceberg table.

- `path`: the expression that represents the path to an element in the VARIANT object. The value of this parameter is a string. The path syntax is similar to JSON path:
  - `$` represents the root element
  - `.` is used to access object fields
  - `[index]` is used to access array elements (0-based indexing)
  - Field names with special characters can be quoted: `$."field.name"`

## Return value

Returns a VARCHAR value.

If the element does not exist, the path is invalid, or the value cannot be converted to a string, the function returns NULL.

## Examples

Example 1: Extract string fields from the root level.

```SQL
SELECT
    get_variant_string(data, '$.kind') AS kind,
    get_variant_string(data, '$.did') AS did
FROM bluesky
LIMIT 3;
```

```plaintext
+--------+--------------------------------------+
| kind   | did                                  |
+--------+--------------------------------------+
| commit | did:plc:gw3yid42zz6qx6gxukvkgxgq     |
| commit | did:plc:jbaujn4dd466ebt6pdf5vxod     |
| commit | did:plc:ytgql26s6zoifhlnvf7qheea     |
+--------+--------------------------------------+
```

Example 2: Extract strings from nested fields.

```SQL
SELECT
    get_variant_string(data, '$.commit.collection') AS collection,
    get_variant_string(data, '$.commit.operation') AS operation
FROM bluesky
WHERE get_variant_string(data, '$.commit.operation') = 'create'
LIMIT 5;
```

```plaintext
+------------------------+-----------+
| collection             | operation |
+------------------------+-----------+
| app.bsky.feed.post     | create    |
| app.bsky.graph.follow  | create    |
| app.bsky.feed.repost   | create    |
| app.bsky.feed.repost   | create    |
| app.bsky.feed.like     | create    |
+------------------------+-----------+
```

Example 3: Extract deeply nested string values.

```SQL
SELECT get_variant_string(data, '$.commit.record.text') AS post_text
FROM bluesky
WHERE get_variant_string(data, '$.commit.collection') = 'app.bsky.feed.post'
  AND get_variant_string(data, '$.commit.record.text') IS NOT NULL
LIMIT 3;
```

```plaintext
+---------------------------------------------------------------------------------+
| post_text                                                                       |
+---------------------------------------------------------------------------------+
| I went on a walk today                                                          |
| Are you interested in developing your own graph sequence model based on a sp... |
| Tam im tiež celkom slušne práši...                                              |
+---------------------------------------------------------------------------------+
```

Example 4: Use in aggregation queries.

```SQL
SELECT
    get_variant_string(data, '$.commit.collection') AS collection,
    COUNT(*) AS count
FROM bluesky
GROUP BY collection
ORDER BY count DESC
LIMIT 5;
```

```plaintext
+------------------------+---------+
| collection             | count   |
+------------------------+---------+
| app.bsky.feed.like     | 5067917 |
| app.bsky.graph.follow  | 2931757 |
| app.bsky.feed.post     | 960723  |
| app.bsky.feed.repost   | 666364  |
| app.bsky.graph.block   | 196053  |
+------------------------+---------+
```

Example 5: Use in WHERE clause for filtering.

```SQL
SELECT
    get_variant_string(data, '$.commit.record.subject') AS follow_subject
FROM bluesky
WHERE get_variant_string(data, '$.commit.collection') = 'app.bsky.graph.follow'
  AND get_variant_string(data, '$.commit.operation') = 'create'
LIMIT 5;
```

```plaintext
+--------------------------------------+
| follow_subject                       |
+--------------------------------------+
| did:plc:rhqpfbdmabrwrd3o552tlsy7     |
| did:plc:vlq5rxxanqrzrqdcjfoirgfz     |
| did:plc:iefkhz4tg3vldzmtgyqkj3xb     |
| did:plc:2phsrz5r5kynlq6pnrrzvrno     |
| did:plc:hxz45fftjaa62vnwiiwh6fhj     |
+--------------------------------------+
```

## keyword

GET_VARIANT_STRING,VARIANT
