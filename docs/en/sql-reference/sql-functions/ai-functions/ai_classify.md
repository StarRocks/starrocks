---
displayed_sidebar: docs
description: "Asks the model to classify text into one of the given categories."
products: [oss, enterprise, cloud, anywhere]
---

# ai_classify

Asks the model to classify text into one of the given categories.

## Syntax

```sql
ai_classify(input, categories[, provider[, model]])
```

## Arguments

- `input`: A VARCHAR expression containing the text to classify.
- `categories`: A nonempty constant `ARRAY<VARCHAR>` containing the candidate categories.

For the types and default behavior of `provider` and `model`, see [Common calling rules](ai_functions.mdx#common-calling-rules).

## Return value

Returns a nullable JSON object. The requested structure is `{"labels":["selected category"]}`, for example, `{"labels":["support"]}`.

StarRocks checks that the result is a JSON object, but does not validate the number of labels, category membership, or classification accuracy.

## Examples

```sql
SELECT ai_classify('Please reset my password.', ['support', 'sales']);

SELECT ai_classify(
    input => 'Please reset my password.',
    categories => ['support', 'sales'],
    provider => 'support_chat'
);
```
