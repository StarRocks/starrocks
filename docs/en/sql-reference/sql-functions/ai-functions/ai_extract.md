---
displayed_sidebar: docs
description: "Extracts information from text using the given field names."
products: [oss, enterprise, cloud, anywhere]
---

# ai_extract

Extracts information from text using the given field names.

## Syntax

```sql
ai_extract(input, labels[, provider[, model]])
```

## Arguments

- `input`: A VARCHAR expression containing the text to extract information from.
- `labels`: A nonempty constant `ARRAY<VARCHAR>` containing the field names to extract.

For the types and default behavior of `provider` and `model`, see [Common calling rules](ai_functions.mdx#common-calling-rules).

## Return value

Returns a nullable JSON object. The requested structure is `{"response":{"field name":"value"}}`, with JSON null requested for fields that are not found. Example result: `{"response":{"name":"Alice","city":"Paris"}}`.

StarRocks checks that the result is a JSON object, but does not validate the field structure of response or the accuracy of the extracted content.

## Examples

```sql
SELECT ai_extract('Alice lives in Paris.', ['name', 'city']);

SELECT ai_extract(
    input => 'Alice lives in Paris.',
    labels => ['name', 'city'],
    provider => 'support_chat'
);
```
