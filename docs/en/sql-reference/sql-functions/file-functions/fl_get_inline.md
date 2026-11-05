---
displayed_sidebar: docs
description: "Extracts the inline field of a FILE value."
sidebar_position: 80
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# fl_get_inline

<Experimental />

Extracts the `inline` field of a [FILE](../../data-types/other-data-types/FILE.md) value.

## Syntax

```sql
VARBINARY fl_get_inline(FILE file_expr)
```

## Parameters

`file_expr`: a FILE expression, such as the result of [to_file](to_file.md) or [try_to_file](try_to_file.md).

## Return value

Returns a VARBINARY value. Returns NULL if the FILE value or the `inline` field is NULL. The field is returned unchanged, with no defaults. Empty bytes and embedded zero bytes are preserved; binary content is neither encoded nor decoded.

## Examples

### Example 1: Extract the field

```sql
SELECT hex(fl_get_inline(to_file(named_struct('inline', to_binary('610062', 'hex'))))) AS inline_hex;
```

```plaintext
+------------+
| inline_hex |
+------------+
| 610062     |
+------------+
```

### Example 2: The field is NULL

```sql
SELECT hex(fl_get_inline(to_file(named_struct('uri', 'a.jpg')))) AS inline_hex;
```

```plaintext
+------------+
| inline_hex |
+------------+
| NULL       |
+------------+
```

## keyword

FL_GET_INLINE, FILE
