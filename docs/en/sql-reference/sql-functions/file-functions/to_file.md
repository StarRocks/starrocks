---
displayed_sidebar: docs
description: "Constructs a FILE value from a STRUCT expression."
sidebar_position: 10
products: [oss, enterprise, cloud, anywhere]
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# to_file

<Experimental />

Constructs a temporary [FILE](../../data-types/other-data-types/FILE.md) value from a STRUCT expression, such as `named_struct(...)` or a STRUCT column. This function does not access storage or infer metadata.

## Syntax

```sql
FILE to_file(STRUCT struct_expr)
```

## Parameters

`struct_expr`: a STRUCT expression that describes the file. The following fields are accepted:

| Field | Accepted input types | FILE field type |
|---|---|---|
| `uri`, `content_type`, `checksum` | CHAR, VARCHAR | VARCHAR |
| `offset`, `size` | TINYINT, SMALLINT, INT, BIGINT | BIGINT |
| `inline` | VARBINARY | VARBINARY |

## Return value

Returns a FILE value. If `struct_expr` is NULL, returns NULL.

## Usage notes

- Field names are case-insensitive and can appear in any order. Unknown or duplicate names cause an analysis error.
- Missing fields and explicit NULL values become NULL. No defaults are filled in, including for `offset` and `size`.
- For each non-NULL STRUCT, exactly one of `uri` and `inline` must be non-NULL. Missing fields count as NULL.
- Integer inputs are widened to BIGINT. Untyped NULL is accepted for every field. Other input types require an explicit CAST.
- For URI references, `offset` and `size` can be NULL; non-NULL values must be greater than or equal to zero. For inline content, both fields must be NULL. A row that violates these rules or the `uri`/`inline` rule causes the query to fail. To return NULL for such rows instead, use [try_to_file](try_to_file.md).
- URI syntax, MIME types, checksums, and the referenced byte range are not validated.
- `inline` contains raw bytes. Empty strings and empty binary values count as non-NULL and are preserved.

## Examples

### Example 1: Construct a file reference

```sql
SELECT to_file(named_struct('uri', 'https://example.com/a.jpg', 'content_type', 'image/jpeg')) AS f;
```

```plaintext
+-------------------------------------------------------------------------------------------------------------------------+
| f                                                                                                                       |
+-------------------------------------------------------------------------------------------------------------------------+
| {"uri":"https://example.com/a.jpg","offset":null,"size":null,"content_type":"image/jpeg","checksum":null,"inline":null} |
+-------------------------------------------------------------------------------------------------------------------------+
```

### Example 2: Construct inline content

```sql
SELECT to_file(named_struct('inline', to_binary('610062', 'hex'))) AS f;
```

```plaintext
+----------------------------------------------------------------------------------------------+
| f                                                                                            |
+----------------------------------------------------------------------------------------------+
| {"uri":null,"offset":null,"size":null,"content_type":null,"checksum":null,"inline":"610062"} |
+----------------------------------------------------------------------------------------------+
```

### Example 3: NULL argument

```sql
SELECT to_file(NULL) AS f;
```

```plaintext
+------+
| f    |
+------+
| NULL |
+------+
```

## keyword

TO_FILE, FILE
