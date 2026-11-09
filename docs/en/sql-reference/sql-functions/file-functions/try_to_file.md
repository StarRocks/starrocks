---
displayed_sidebar: docs
description: "Constructs a FILE value from a STRUCT expression and returns NULL for rows with invalid field values."
sidebar_position: 20
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# try_to_file

<Experimental />

Constructs a temporary FILE using the same fields, types, and validation rules as [to_file](to_file.md). Only rows with invalid field values become NULL; other rows are unaffected.

## Syntax

```sql
FILE try_to_file(STRUCT struct_expr)
```

## Parameters

`struct_expr`: the same as in [to_file](to_file.md).

## Return value

Returns a FILE value. If conversion to FILE fails, returns NULL.

## Usage notes

- Unknown or duplicate fields, unsupported types, and invalid argument counts still cause analysis errors. Errors evaluating the argument and internal or resource errors are not converted to NULL.

## Examples

### Example 1: Valid input

```sql
SELECT try_to_file(named_struct('uri', 'a.jpg', 'size', 12)) AS f;
```

```plaintext
+-------------------------------------------------------------------------------------------+
| f                                                                                         |
+-------------------------------------------------------------------------------------------+
| {"uri":"a.jpg","offset":null,"size":12,"content_type":null,"checksum":null,"inline":null} |
+-------------------------------------------------------------------------------------------+
```

### Example 2: Invalid rows return NULL

```sql
SELECT
    try_to_file(named_struct('uri', 'a.jpg', 'size', -1)) IS NULL AS negative_size,
    try_to_file(named_struct('uri', NULL, 'inline', NULL)) IS NULL AS neither_set,
    try_to_file(named_struct('uri', 'a.jpg', 'inline', to_binary('61', 'hex'))) IS NULL AS both_set;
```

```plaintext
+---------------+-------------+----------+
| negative_size | neither_set | both_set |
+---------------+-------------+----------+
| 1             | 1           | 1        |
+---------------+-------------+----------+
```

## keyword

TRY_TO_FILE, FILE
