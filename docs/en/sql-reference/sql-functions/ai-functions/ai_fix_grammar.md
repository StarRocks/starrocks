---
displayed_sidebar: docs
description: "Corrects grammar and spelling, asking the model to preserve the original meaning and tone."
products: [oss, enterprise, cloud, anywhere]
---

# ai_fix_grammar

Corrects grammar and spelling, asking the model to preserve the original meaning and tone.

## Syntax

```sql
ai_fix_grammar(input[, provider[, model]])
```

## Arguments

- `input`: A VARCHAR expression containing the text to correct.

For the types and default behavior of `provider` and `model`, see [Common calling rules](ai_functions.mdx#common-calling-rules).

## Return value

Returns a nullable VARCHAR containing the corrected text.

## Examples

```sql
SELECT ai_fix_grammar('She go to school every day.');

SELECT ai_fix_grammar(input => 'She go to school every day.', provider => 'support_chat');
```
