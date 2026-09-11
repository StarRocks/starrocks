---
displayed_sidebar: docs
description: "Use SYSTEM chat models to classify, extract, translate, summarize, and transform text."
sidebar_position: 21
---

# AI text functions

These functions use the SYSTEM chat endpoint and credentials configured for [ai_complete](ai_complete.md#configuration). They build task-specific prompts and return typed results. Upgrade all FE and BE nodes before using these new functions.

:::warning
Input text is sent to the model provider and can incur charges. Use only approved endpoints and data. In particular, `ai_redact` sends the original, unredacted text to the provider; it is not a local privacy filter.
:::

## Syntax

Each function has exactly two forms: use the default chat model, or supply a model as the first argument. These helpers do not accept an options MAP.

```sql
ai_sentiment(text)
ai_sentiment(model, text)
ai_classify(text, categories)
ai_classify(model, text, categories)
ai_extract(text, keys)
ai_extract(model, text, keys)
ai_fix_grammar(text)
ai_fix_grammar(model, text)
ai_redact(text, categories)
ai_redact(model, text, categories)
ai_translate(text, source_language, target_language)
ai_translate(model, text, source_language, target_language)
ai_similarity(text1, text2)
ai_similarity(model, text1, text2)
ai_summarize(text)
ai_summarize(model, text)
ai_filter(text, condition)
ai_filter(model, text, condition)
```

Text, model, condition, and language arguments are VARCHAR expressions. Explicit models can vary by row. A constant explicit model must not be blank. Without an explicit model, `ai_default_chat_model` must be configured.

`categories` and `keys` must be constant, nonempty `ARRAY<VARCHAR>` values with no NULL or blank elements. A VARCHAR containing a URL remains text: StarRocks does not fetch the URL. FILE, image, and multimodal inputs are not supported.

## Return values

All results are nullable and non-deterministic.

| Function | Type | Meaning |
|----------|------|---------|
| `ai_sentiment` | VARCHAR | One normalized value: `positive`, `negative`, `neutral`, `mixed`, or `unknown`. |
| `ai_classify` | JSON | Asks for one category in an object such as `{"labels":["category"]}`. |
| `ai_extract` | JSON | Asks for an object such as `{"response":{"key":"value"}}`, with NULL for missing values. |
| `ai_fix_grammar` | VARCHAR | Corrected text. |
| `ai_redact` | VARCHAR | Text with detected values replaced by category markers such as `[EMAIL]`. |
| `ai_translate` | VARCHAR | Translated text. NULL or an empty source language requests automatic detection. A NULL or empty target language returns NULL without a request. |
| `ai_similarity` | FLOAT | A model-generated semantic similarity score from 0 through 1, not a vector-distance calculation. |
| `ai_summarize` | VARCHAR | A text summary. |
| `ai_filter` | BOOLEAN | Whether the model considers the text to satisfy the condition. |

Classification and extraction responses must be valid JSON objects. StarRocks does not enforce the requested object schema or verify the factual accuracy of model output.

NULL text or an explicitly supplied NULL model returns NULL without a provider request. For translation, NULL source language is the exception described above. Malformed typed responses and other row-level failures follow [ai_function_on_error](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error): `ignore` returns NULL and `fail` aborts the query. Configuration errors, cancellation, and deadlines are not converted into NULL.

The [SQL placement restrictions, prepared-statement replanning, and runtime limits of ai_complete](ai_complete.md#limitations-and-security) also apply to these functions.

## Examples

These examples only plan the queries. `EXPLAIN` requires valid SYSTEM chat configuration but does not submit provider requests.

```sql
EXPLAIN SELECT ai_sentiment('The delivery was excellent.');
EXPLAIN SELECT ai_classify('Please reset my password.', ['support', 'sales']);
EXPLAIN SELECT ai_extract('Order 42 ships Friday.', ['order', 'ship_date']);
EXPLAIN SELECT ai_translate('Hello', NULL, 'Chinese');
EXPLAIN SELECT ai_similarity('A quick reply', 'A fast response');
EXPLAIN SELECT ai_filter('The package arrived damaged.', 'describes a damaged item');
EXPLAIN SELECT ai_summarize('approved-chat-model', 'A local test paragraph.');
```

For text embeddings, see [ai_embed](ai_embed.md). For named-model chat or embeddings, see [AI models](ai_model.md).
