---
displayed_sidebar: docs
description: "Extracts the content_type field of a FILE value."
sidebar_position: 60
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# fl_get_content_type

<Experimental />

Extracts the `content_type` field of a [FILE](../../data-types/other-data-types/FILE.md) value.

## Syntax

```sql
VARCHAR fl_get_content_type(FILE file_expr)
```

## Parameters

`file_expr`: a FILE expression, such as the result of [to_file](to_file.md) or [try_to_file](try_to_file.md).

## Return value

Returns a VARCHAR value. Returns NULL if the FILE value or the `content_type` field is NULL. The field is returned unchanged, with no defaults. Empty strings are preserved.

## Examples

### Example 1: Extract the field

```sql
SELECT fl_get_content_type(to_file(named_struct('uri', 'a.jpg', 'content_type', 'image/jpeg'))) AS content_type;
```

```plaintext
+--------------+
| content_type |
+--------------+
| image/jpeg   |
+--------------+
```

### Example 2: The field is NULL

```sql
SELECT fl_get_content_type(to_file(named_struct('uri', 'a.jpg'))) AS content_type;
```

```plaintext
+--------------+
| content_type |
+--------------+
| NULL         |
+--------------+
```

## keyword

FL_GET_CONTENT_TYPE, FILE
