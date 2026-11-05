---
displayed_sidebar: docs
description: "Construct FILE values and extract file metadata and inline content with getter functions."
sidebar_position: 30
toc_max_heading_level: 2
---

# FILE functions

These functions construct [FILE](../../data-types/other-data-types/FILE.md) values or extract their fields. They do not access storage or infer metadata.

| Function | Purpose | Return type |
| --- | --- | --- |
| [to_file](#to_file) | Construct a FILE from a STRUCT; reject invalid field values | FILE |
| [try_to_file](#try_to_file) | Construct a FILE from a STRUCT; return NULL for invalid rows | FILE |
| [fl_get_uri](#fl_get_uri) | Extract the file location | VARCHAR |
| [fl_get_offset](#fl_get_offset) | Extract the starting byte offset | BIGINT |
| [fl_get_size](#fl_get_size) | Extract the byte count | BIGINT |
| [fl_get_content_type](#fl_get_content_type) | Extract the media type | VARCHAR |
| [fl_get_checksum](#fl_get_checksum) | Extract the checksum | VARCHAR |
| [fl_get_inline](#fl_get_inline) | Extract inline bytes | VARBINARY |

## Common behavior

`FL_GET_*` accepts a FILE expression, including the result of [TO_FILE](#to_file) or [TRY_TO_FILE](#try_to_file). It returns NULL if the FILE value or the requested field is NULL. Fields are returned unchanged, with no defaults. Empty strings, empty bytes, and embedded zero bytes are preserved; binary content is neither encoded nor decoded.

## to_file

Constructs a temporary FILE value from a STRUCT expression, such as `named_struct(...)` or a STRUCT column. This function does not access storage or infer metadata.

### Syntax

```sql
TO_FILE(struct_expr)
```

### Fields

| Fields | Accepted input types | FILE |
|---|---|---|
| `uri`, `content_type`, `checksum` | CHAR, VARCHAR | VARCHAR |
| `offset`, `size` | TINYINT, SMALLINT, INT, BIGINT | BIGINT |
| `inline` | VARBINARY | VARBINARY |

### Semantics

- Field names are case-insensitive and can appear in any order. Unknown or duplicate names cause an analysis error.
- Missing fields and explicit NULL values become NULL. No defaults are filled in, including for `offset` and `size`.
- A NULL STRUCT returns NULL and skips field-value validation. For each non-NULL STRUCT, exactly one of `uri` and `inline` must be non-NULL. Missing fields count as NULL.
- Integer inputs are widened to BIGINT. Untyped NULL is accepted for every field. Other input types require an explicit CAST.
- `offset` and `size` can be NULL; non-NULL values must be greater than or equal to zero. A row that violates these rules or the `uri`/`inline` rule causes the query to fail.
- URI syntax, MIME types, checksums, and consistency between size and payload length are not validated.
- `inline` contains raw bytes. Empty strings and empty binary values count as non-NULL and are preserved.
- Returns FILE. This overload does not resolve Storage Volumes, send HEAD requests, or generate presigned URLs.

### Examples

```sql
SELECT TO_FILE(named_struct('uri', 'https://example.com/a.jpg', 'content_type', 'image/jpeg'));

SELECT TO_FILE(named_struct('inline', to_binary('610062', 'hex')));

SELECT TO_FILE(named_struct('uri', '', 'offset', 0, 'size', NULL));
SELECT TO_FILE(NULL) IS NULL; -- 1
```

## try_to_file

Constructs a temporary FILE using the same fields, types, and validation rules as [TO_FILE](#to_file). Only rows with invalid field values become NULL; valid rows in the same batch are retained.

### Syntax

```sql
TRY_TO_FILE(struct_expr)
```

### Behavior

- A NULL STRUCT returns NULL. A non-NULL STRUCT returns NULL if `uri` and `inline` are both NULL or both non-NULL, or if a non-NULL `offset` or `size` is negative.
- Unknown or duplicate fields, unsupported types, and invalid argument counts still cause analysis errors. Errors evaluating the argument and internal or resource errors are not converted to NULL.

### Examples

```sql
SELECT FL_GET_URI(TRY_TO_FILE(named_struct('uri', 'a.jpg', 'size', 12)));
-- a.jpg

SELECT HEX(FL_GET_INLINE(TRY_TO_FILE(named_struct('inline', to_binary('610062', 'hex')))));
-- 610062

SELECT TRY_TO_FILE(named_struct('uri', 'a.jpg', 'size', -1)) IS NULL;
-- 1

SELECT TRY_TO_FILE(named_struct('uri', NULL, 'inline', NULL)) IS NULL;
-- 1

SELECT TRY_TO_FILE(named_struct('uri', 'a.jpg', 'inline', to_binary('61', 'hex'))) IS NULL;
-- 1
```

## fl_get_uri

Extracts the `uri` field and returns VARCHAR.

### Syntax

```sql
FL_GET_URI(file_expr)
```

### Examples

```sql
SELECT FL_GET_URI(TO_FILE(named_struct('uri', 'https://example.com/a.jpg')));
-- https://example.com/a.jpg

SELECT FL_GET_URI(NULL) IS NULL;
-- 1
```

## fl_get_offset

Extracts the `offset` field and returns BIGINT.

### Syntax

```sql
FL_GET_OFFSET(file_expr)
```

### Examples

```sql
SELECT FL_GET_OFFSET(TO_FILE(named_struct('uri', 'a.jpg', 'offset', 0)));
-- 0

SELECT FL_GET_OFFSET(NULL) IS NULL;
-- 1
```

## fl_get_size

Extracts the `size` field and returns BIGINT.

### Syntax

```sql
FL_GET_SIZE(file_expr)
```

### Examples

```sql
SELECT FL_GET_SIZE(TO_FILE(named_struct('uri', 'a.jpg', 'size', 12)));
-- 12

SELECT FL_GET_SIZE(NULL) IS NULL;
-- 1
```

## fl_get_content_type

Extracts the `content_type` field and returns VARCHAR.

### Syntax

```sql
FL_GET_CONTENT_TYPE(file_expr)
```

### Examples

```sql
SELECT FL_GET_CONTENT_TYPE(TO_FILE(named_struct('uri', 'a.jpg', 'content_type', 'image/jpeg')));
-- image/jpeg

SELECT FL_GET_CONTENT_TYPE(NULL) IS NULL;
-- 1
```

## fl_get_checksum

Extracts the `checksum` field and returns VARCHAR.

### Syntax

```sql
FL_GET_CHECKSUM(file_expr)
```

### Examples

```sql
SELECT FL_GET_CHECKSUM(TO_FILE(named_struct('uri', 'a.jpg', 'checksum', 'opaque')));
-- opaque

SELECT FL_GET_CHECKSUM(NULL) IS NULL;
-- 1
```

## fl_get_inline

Extracts the `inline` field and returns VARBINARY.

### Syntax

```sql
FL_GET_INLINE(file_expr)
```

### Examples

```sql
SELECT HEX(FL_GET_INLINE(TO_FILE(named_struct('inline', to_binary('610062', 'hex')))));
-- 610062

SELECT FL_GET_INLINE(NULL) IS NULL;
-- 1
```
