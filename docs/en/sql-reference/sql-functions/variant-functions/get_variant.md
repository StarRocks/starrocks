---
displayed_sidebar: docs
---

# get_variant

A family of functions that extract typed values from VARIANT objects at specified paths.

These functions navigate to a specified path in a VARIANT value and return the value as a specific data type. If the value at the path does not exist, the path is invalid, or the value cannot be converted to the requested type, the functions return NULL.

## Syntax

```Haskell
BIGINT get_variant_int(variant_expr, path)
DOUBLE get_variant_double(variant_expr, path)
VARCHAR get_variant_string(variant_expr, path)
BOOLEAN get_variant_bool(variant_expr, path)
```

## Parameters

- `variant_expr`: the expression that represents the VARIANT object. This is typically a VARIANT column from an Iceberg table.

- `path`: the expression that represents the path to an element in the VARIANT object. The value of this parameter is a string. The path syntax is similar to JSON path:
  - `$` represents the root element
  - `.` is used to access object fields
  - `[index]` is used to access array elements (0-based indexing)
  - Field names with special characters can be quoted: `$."field.name"`

## Return values

- `get_variant_int`: Returns a BIGINT value, or NULL if the value cannot be converted to an integer.
- `get_variant_double`: Returns a DOUBLE value, or NULL if the value cannot be converted to a double.
- `get_variant_string`: Returns a VARCHAR value, or NULL if the value cannot be converted to a string.
- `get_variant_bool`: Returns a BOOLEAN value (1 or 0), or NULL if the value cannot be converted to a boolean.

## Examples

### Example 1: Extract basic fields from root level

Extract string and integer values from the Bluesky firehose data:

```SQL
SELECT
    get_variant_string(data, '$.kind') AS kind,
    get_variant_string(data, '$.did') AS did,
    get_variant_int(data, '$.time_us') AS timestamp_us
FROM bluesky
LIMIT 3;
```

```plaintext
+--------+--------------------------------------+------------------+
| kind   | did                                  | timestamp_us     |
+--------+--------------------------------------+------------------+
| commit | did:plc:gw3yid42zz6qx6gxukvkgxgq     | 1733267476040329 |
| commit | did:plc:jbaujn4dd466ebt6pdf5vxod     | 1733267476040803 |
| commit | did:plc:ytgql26s6zoifhlnvf7qheea     | 1733267476041472 |
+--------+--------------------------------------+------------------+
```

### Example 2: Extract nested fields

Extract operation details from nested commit structures:

```SQL
SELECT
    get_variant_string(data, '$.commit.collection') AS collection,
    get_variant_string(data, '$.commit.operation') AS operation,
    get_variant_string(data, '$.commit.rkey') AS record_key
FROM bluesky
WHERE get_variant_string(data, '$.commit.operation') = 'create'
LIMIT 5;
```

```plaintext
+------------------------+-----------+---------------+
| collection             | operation | record_key    |
+------------------------+-----------+---------------+
| app.bsky.feed.post     | create    | 3lckw3k2abc2d |
| app.bsky.graph.follow  | create    | 3lckw3k2def3e |
| app.bsky.feed.repost   | create    | 3lckw3k2ghi4f |
| app.bsky.feed.repost   | create    | 3lckw3k2jkl5g |
| app.bsky.feed.like     | create    | 3lckw3k2mno6h |
+------------------------+-----------+---------------+
```

### Example 3: Extract deeply nested content

Extract post text and metadata from deeply nested structures:

```SQL
SELECT
    get_variant_string(data, '$.commit.record.text') AS post_text,
    get_variant_string(data, '$.commit.record.langs[0]') AS language,
    get_variant_int(data, '$.commit.record.createdAt') AS created_timestamp
FROM bluesky
WHERE get_variant_string(data, '$.commit.collection') = 'app.bsky.feed.post'
  AND get_variant_string(data, '$.commit.record.text') IS NOT NULL
LIMIT 3;
```

```plaintext
+---------------------------------------------------------------------------------+----------+-------------------+
| post_text                                                                       | language | created_timestamp |
+---------------------------------------------------------------------------------+----------+-------------------+
| I went on a walk today                                                          | en       | 1733267476        |
| Are you interested in developing your own graph sequence model based on a sp... | en       | 1733267476        |
| Tam im tiež celkom slušne práši...                                              | sk       | 1733267476        |
+---------------------------------------------------------------------------------+----------+-------------------+
```

### Example 4: Extract boolean values

Extract boolean flags from post records:

```SQL
SELECT
    get_variant_string(data, '$.commit.record.text') AS post_text,
    get_variant_bool(data, '$.commit.record.bridgyOriginalUrl') AS is_bridged,
    get_variant_bool(data, '$.commit.record.selfLabel.settings.adult') AS has_adult_label
FROM bluesky
WHERE get_variant_string(data, '$.commit.collection') = 'app.bsky.feed.post'
LIMIT 5;
```

```plaintext
+------------------------------------+-------------+------------------+
| post_text                          | is_bridged  | has_adult_label  |
+------------------------------------+-------------+------------------+
| Great day for coding!              | NULL        | 0                |
| Check out this amazing view        | 1           | 0                |
| Working on my new project          | NULL        | NULL             |
| Another beautiful sunset           | NULL        | 0                |
| Weekend plans anyone?              | 0           | NULL             |
+------------------------------------+-------------+------------------+
```

### Example 5: Extract numeric values with calculations

Convert microseconds to seconds and perform timestamp calculations:

```SQL
SELECT
    get_variant_int(data, '$.time_us') AS timestamp_us,
    get_variant_int(data, '$.time_us') / 1000000 AS timestamp_seconds,
    FROM_UNIXTIME(get_variant_int(data, '$.time_us') / 1000000) AS readable_time
FROM bluesky
LIMIT 3;
```

```plaintext
+------------------+-------------------+---------------------+
| timestamp_us     | timestamp_seconds | readable_time       |
+------------------+-------------------+---------------------+
| 1733267476040329 | 1733267476        | 2024-12-03 20:17:56 |
| 1733267476040803 | 1733267476        | 2024-12-03 20:17:56 |
| 1733267476041472 | 1733267476        | 2024-12-03 20:17:56 |
+------------------+-------------------+---------------------+
```

### Example 6: Use in aggregation queries

Count records by collection type:

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

### Example 7: Count records by operation type

```SQL
SELECT
    get_variant_string(data, '$.commit.operation') AS operation,
    COUNT(*) AS count
FROM bluesky
GROUP BY operation;
```

```plaintext
+-----------+---------+
| operation | count   |
+-----------+---------+
| delete    | 420223  |
| update    | 40283   |
| NULL      | 39361   |
| create    | 9500118 |
+-----------+---------+
```

### Example 8: Extract and analyze engagement metrics

Extract numeric engagement metrics from posts:

```SQL
SELECT
    get_variant_string(data, '$.commit.record.text') AS post_text,
    get_variant_int(data, '$.commit.record.likeCount') AS likes,
    get_variant_int(data, '$.commit.record.replyCount') AS replies,
    get_variant_int(data, '$.commit.record.repostCount') AS reposts,
    get_variant_double(data, '$.commit.record.engagementRate') AS engagement_rate
FROM bluesky
WHERE get_variant_string(data, '$.commit.collection') = 'app.bsky.feed.post'
  AND get_variant_int(data, '$.commit.record.likeCount') > 100
LIMIT 5;
```

```plaintext
+----------------------------------------+-------+---------+---------+-----------------+
| post_text                              | likes | replies | reposts | engagement_rate |
+----------------------------------------+-------+---------+---------+-----------------+
| This is amazing!                       | 245   | 23      | 45      | 0.12            |
| Can't believe this happened            | 189   | 12      | 34      | 0.08            |
| Just finished my project               | 156   | 8       | 19      | 0.05            |
| Beautiful day at the park              | 134   | 15      | 28      | 0.07            |
| Check out this cool thing I made       | 112   | 6       | 11      | 0.04            |
+----------------------------------------+-------+---------+---------+-----------------+
```

### Example 9: Combine multiple variant functions in filtering

Find verified accounts that are actively posting:

```SQL
SELECT
    get_variant_string(data, '$.did') AS user_did,
    get_variant_string(data, '$.commit.record.text') AS post_text,
    get_variant_bool(data, '$.commit.record.verified') AS is_verified
FROM bluesky
WHERE get_variant_string(data, '$.commit.collection') = 'app.bsky.feed.post'
  AND get_variant_bool(data, '$.commit.record.verified') = TRUE
  AND get_variant_int(data, '$.time_us') > 1733267476000000
LIMIT 5;
```

```plaintext
+--------------------------------------+----------------------------------+-------------+
| user_did                             | post_text                        | is_verified |
+--------------------------------------+----------------------------------+-------------+
| did:plc:rhqpfbdmabrwrd3o552tlsy7     | Official announcement coming up  | 1           |
| did:plc:vlq5rxxanqrzrqdcjfoirgfz     | New feature release today        | 1           |
| did:plc:iefkhz4tg3vldzmtgyqkj3xb     | Thank you all for your support   | 1           |
| did:plc:2phsrz5r5kynlq6pnrrzvrno     | Excited to share this news       | 1           |
| did:plc:hxz45fftjaa62vnwiiwh6fhj     | Join us for the live stream      | 1           |
+--------------------------------------+----------------------------------+-------------+
```

### Example 10: NULL handling

Returns NULL when paths don't exist or values can't be converted:

```SQL
SELECT
    get_variant_int(data, '$.nonexistent.field') AS missing_int,
    get_variant_double(data, '$.kind') AS string_as_double,
    get_variant_bool(data, '$.time_us') AS int_as_bool,
    get_variant_string(data, '$.commit.record.nonexistent') AS missing_string
FROM bluesky
LIMIT 1;
```

```plaintext
+-------------+------------------+--------------+----------------+
| missing_int | string_as_double | int_as_bool  | missing_string |
+-------------+------------------+--------------+----------------+
| NULL        | NULL             | NULL         | NULL           |
+-------------+------------------+--------------+----------------+
```

## keyword

GET_VARIANT_INT,GET_VARIANT_DOUBLE,GET_VARIANT_STRING,GET_VARIANT_BOOL,VARIANT
