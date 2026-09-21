---
displayed_sidebar: docs
description: "FILE is a read-only scalar type that references a file, or a byte range of a file, through a fixed set of fields."
---

# FILE

FILE is a scalar type that describes a file, or a byte range inside a file. A FILE value is read and returned as a whole: it has a fixed set of fields, but the fields are not individually addressable in SQL.

FILE currently cannot be persisted in StarRocks; FILE values can only be obtained from external catalogs.

## Structure

Every FILE value carries the same fields, in this order. Each field can be NULL.

| Field          | Type      | Description                                                                                  |
|----------------|-----------|----------------------------------------------------------------------------------------------|
| `uri`          | VARCHAR   | Location of the file, for example an object storage or HDFS path.                            |
| `offset`       | BIGINT    | Byte position inside the file where the referenced content starts.                           |
| `size`         | BIGINT    | Number of bytes of the referenced content, counted from `offset`.                           |
| `content_type` | VARCHAR   | Media type of the content, such as `image/png`.                                              |
| `checksum`     | VARCHAR   | Checksum of the referenced content.                                                          |
| `inline`       | VARBINARY | The content itself, stored directly in the value instead of being referenced through `uri`. |

A FILE value is one of the following:

- A **reference**: `uri` is set, usually together with `offset` and `size`, and `inline` is NULL.
- **Inline content**: `inline` holds the bytes and `uri`, `offset`, and `size` are NULL.

## Output format

A FILE value is rendered as a JSON-like object that lists all fields:

```plain text
{"uri":"s3://bucket/images/a.png","offset":4,"size":8,"content_type":null,"checksum":null,"inline":null}
{"uri":null,"offset":null,"size":null,"content_type":null,"checksum":null,"inline":"89504e470d0a1a0a"}
```

The `inline` bytes are encoded according to the session variable [`binary_encoding_format`](../../System_variable.md#binary_encoding_format).

## Limitations

- FILE cannot be used as a column type in CREATE TABLE, CREATE TABLE AS SELECT, or materialized views.
- FILE values cannot be compared, sorted, grouped, joined on, used with DISTINCT, cast to or from other types, or passed to window functions.
- `IS NULL` and `IS NOT NULL` are supported.
