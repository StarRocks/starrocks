---
displayed_sidebar: docs
description: "Analyzes the overall sentiment of text."
products: [oss, enterprise, cloud, anywhere]
---

# ai_sentiment

Analyzes the overall sentiment of text.

## Syntax

```sql
ai_sentiment(input[, provider[, model]])
```

## Arguments

- `input`: A VARCHAR expression containing the text to analyze.

For the types and default behavior of `provider` and `model`, see [Common calling rules](ai_functions.mdx#common-calling-rules).

## Return value

Returns a nullable VARCHAR. Valid results are normalized to one of `positive`, `negative`, `neutral`, `mixed`, or `unknown`. Other responses are treated as row-level errors.

## Examples

```sql
SELECT ai_sentiment('The delivery was excellent.');

SELECT ai_sentiment(input => 'The delivery was excellent.', provider => 'support_chat');
```
