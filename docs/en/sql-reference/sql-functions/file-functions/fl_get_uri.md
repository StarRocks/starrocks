---
displayed_sidebar: docs
description: "Extracts the uri field of a FILE value."
sidebar_position: 30
products: [oss, enterprise, cloud, anywhere]
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# fl_get_uri

<Experimental />

Extracts the `uri` field of a [FILE](../../data-types/other-data-types/FILE.md) value.

## Syntax

```sql
VARCHAR fl_get_uri(FILE file_expr)
```

## Parameters

`file_expr`: a FILE expression, such as the result of [to_file](to_file.md) or [try_to_file](try_to_file.md).

## Return value

Returns a VARCHAR value. Returns NULL if the FILE value or the `uri` field is NULL. The field is returned unchanged, with no defaults. Empty strings are preserved.

## Examples

### Example 1: Extract the field

```sql
SELECT fl_get_uri(to_file(named_struct('uri', 'https://example.com/a.jpg'))) AS uri;
```

```plaintext
+---------------------------+
| uri                       |
+---------------------------+
| https://example.com/a.jpg |
+---------------------------+
```

### Example 2: The field is NULL

```sql
SELECT fl_get_uri(to_file(named_struct('inline', to_binary('61', 'hex')))) AS uri;
```

```plaintext
+------+
| uri  |
+------+
| NULL |
+------+
```

## keyword

FL_GET_URI, FILE
