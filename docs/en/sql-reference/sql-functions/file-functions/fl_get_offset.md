---
displayed_sidebar: docs
description: "Extracts the offset field of a FILE value."
sidebar_position: 40
products: [oss, enterprise, cloud, anywhere]
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# fl_get_offset

<Experimental />

Extracts the `offset` field of a [FILE](../../data-types/other-data-types/FILE.md) value.

## Syntax

```sql
BIGINT fl_get_offset(FILE file_expr)
```

## Parameters

`file_expr`: a FILE expression, such as the result of [to_file](to_file.md) or [try_to_file](try_to_file.md).

## Return value

Returns a BIGINT value. Returns NULL if the FILE value or the `offset` field is NULL. The field is returned unchanged, with no defaults.

## Examples

### Example 1: Extract the field

```sql
SELECT fl_get_offset(to_file(named_struct('uri', 'a.jpg', 'offset', 0))) AS file_offset;
```

```plaintext
+-------------+
| file_offset |
+-------------+
| 0           |
+-------------+
```

### Example 2: The field is NULL

```sql
SELECT fl_get_offset(to_file(named_struct('uri', 'a.jpg'))) AS file_offset;
```

```plaintext
+-------------+
| file_offset |
+-------------+
| NULL        |
+-------------+
```

## keyword

FL_GET_OFFSET, FILE
