---
displayed_sidebar: docs
description: "Extracts the checksum field of a FILE value."
sidebar_position: 70
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# fl_get_checksum

<Experimental />

Extracts the `checksum` field of a [FILE](../../data-types/other-data-types/FILE.md) value.

## Syntax

```sql
VARCHAR fl_get_checksum(FILE file_expr)
```

## Parameters

`file_expr`: a FILE expression, such as the result of [to_file](to_file.md) or [try_to_file](try_to_file.md).

## Return value

Returns a VARCHAR value. Returns NULL if the FILE value or the `checksum` field is NULL. The field is returned unchanged, with no defaults. Empty strings are preserved.

## Examples

### Example 1: Extract the field

```sql
SELECT fl_get_checksum(to_file(named_struct('uri', 'a.jpg', 'checksum', 'opaque'))) AS checksum;
```

```plaintext
+----------+
| checksum |
+----------+
| opaque   |
+----------+
```

### Example 2: The field is NULL

```sql
SELECT fl_get_checksum(to_file(named_struct('uri', 'a.jpg'))) AS checksum;
```

```plaintext
+----------+
| checksum |
+----------+
| NULL     |
+----------+
```

## keyword

FL_GET_CHECKSUM, FILE
