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

Returns a nullable JSON object. StarRocks asks the model to choose one category from `categories` and put it in the `labels` array, for example, `{"labels":["support"]}`.

StarRocks only checks that the result is a JSON object. It does not check the length of `labels`, whether the category appears in `categories`, or whether the classification is correct.

## Examples

```sql
SELECT ai_classify('Please reset my password.', ['support', 'sales']);

SELECT ai_classify(
    input => 'Please reset my password.',
    categories => ['support', 'sales'],
    provider => 'support_chat'
);
```
