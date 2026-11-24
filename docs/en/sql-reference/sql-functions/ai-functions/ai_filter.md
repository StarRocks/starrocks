---
displayed_sidebar: docs
description: "Determines whether text meets a condition expressed in natural language."
products: [oss, enterprise, cloud, anywhere]
---

# ai_filter

Determines whether text meets a condition expressed in natural language.

## Syntax

```sql
ai_filter(input, condition[, provider[, model]])
```

## Arguments

- `input`: A VARCHAR expression containing the text to evaluate.
- `condition`: A VARCHAR expression describing the condition in natural language.

For the types and default behavior of `provider` and `model`, see [Common calling rules](ai_functions.mdx#common-calling-rules).

## Return value

Returns a nullable BOOLEAN. The model response must normalize to `true` or `false`. Other responses are treated as row-level errors.

## Examples

```sql
SELECT ai_filter('The package arrived broken.', 'The customer reports damaged goods.');

SELECT ai_filter(
    input => 'The package arrived broken.',
    condition => 'The customer reports damaged goods.',
    provider => 'support_chat'
);
```
