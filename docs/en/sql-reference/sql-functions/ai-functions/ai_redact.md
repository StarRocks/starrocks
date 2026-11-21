---
displayed_sidebar: docs
description: "Asks the model to identify sensitive information in the specified categories and replace it with category markers."
products: [oss, enterprise, cloud, anywhere]
---

# ai_redact

Asks the model to identify sensitive information in the specified categories and replace it with category markers.

## Syntax

```sql
ai_redact(input, categories[, provider[, model]])
```

## Arguments

- `input`: A VARCHAR expression containing the original text to process.
- `categories`: A nonempty constant `ARRAY<VARCHAR>` containing the categories to redact, such as `name`, `email`, and `phone`.

For the types and default behavior of `provider` and `model`, see [Common calling rules](ai_functions.mdx#common-calling-rules).

## Return value

Returns a nullable VARCHAR. The model is instructed to replace identified information with the uppercase category name in square brackets, such as `[EMAIL]`, and return the original text if no matching information is found.

The original text is sent to the model service. This function does not redact data locally before transmission, and the model may miss sensitive information.

## Examples

```sql
SELECT ai_redact('Contact Alice at alice@example.com.', ['name', 'email']);

SELECT ai_redact(
    input => 'Contact Alice at alice@example.com.',
    categories => ['name', 'email'],
    provider => 'support_chat'
);
```
