---
displayed_sidebar: docs
description: "Use AI Providers to classify, extract, translate, summarize, and transform text."
sidebar_position: 21
---

# AI text functions

Text helpers build task-specific prompts through a `chat` Provider using the `openai` protocol. Each supports a default model, an explicit model, or an explicit Provider and model. These helpers do not accept options MAPs.

Both `USAGE ON AI FUNCTION <function_name>` and `USAGE ON AI PROVIDER <provider_name>` are required, including for the selected default Provider. See [AI function privileges](ai_functions.mdx#ai-function-privileges).

## Syntax

```sql
ai_sentiment(text)
ai_sentiment(model, text)
ai_sentiment(provider, model, text)
ai_classify(text, categories)
ai_classify(model, text, categories)
ai_classify(provider, model, text, categories)
ai_extract(text, keys)
ai_extract(model, text, keys)
ai_extract(provider, model, text, keys)
ai_fix_grammar(text)
ai_fix_grammar(model, text)
ai_fix_grammar(provider, model, text)
ai_redact(text, categories)
ai_redact(model, text, categories)
ai_redact(provider, model, text, categories)
ai_translate(text, source_language, target_language)
ai_translate(model, text, source_language, target_language)
ai_translate(provider, model, text, source_language, target_language)
ai_similarity(text1, text2)
ai_similarity(model, text1, text2)
ai_similarity(provider, model, text1, text2)
ai_summarize(text)
ai_summarize(model, text)
ai_summarize(provider, model, text)
ai_filter(text, condition)
ai_filter(model, text, condition)
ai_filter(provider, model, text, condition)
```

## Parameters

Omitting `provider` selects the cluster default Provider for the function type: `chat` for completion and text helpers, `embedding` for embeddings. Omitting `model` uses that Provider's required default model. An explicit model takes precedence and may vary by row. `provider` must be a nonblank constant string; a constant explicit model must not be blank. An explicit Provider call does not require a cluster default.

Text, model, condition, and language arguments are VARCHAR expressions. `categories` and `keys` must be constant, nonempty `ARRAY<VARCHAR>` values without NULL or blank elements. FILE inputs are unsupported. `ai_redact` sends the original text to the model; it is not a local privacy filter.

NULL text, explicit model, or FILE input returns NULL without a model request, whether it is a constant or a runtime column value. A NULL options MAP means no options; a NULL translation source language requests automatic detection. Provider names and helper arrays retain their parameter validation. Nested NULL values inside options retain their JSON-compatible semantics. If an untyped NULL matches different argument types across overloads, specify its type with `CAST`, for example `CAST(NULL AS VARCHAR)` or `CAST(NULL AS MAP<VARCHAR, JSON>)`.

Use language codes such as `en`, `zh`, and `fr`. A NULL or empty source language (`''`) requests automatic detection. A NULL target language returns NULL without a request. An empty or ASCII-whitespace-only target language also returns NULL without a request. No fixed language whitelist or `return_error_details` parameter is provided.

## Return value

| Function | Type | Meaning |
|----------|------|---------|
| `ai_sentiment` | VARCHAR | One normalized value: `positive`, `negative`, `neutral`, `mixed`, or `unknown`. |
| `ai_classify` | JSON | Asks for one category in an object such as `{"labels":["category"]}`. |
| `ai_extract` | JSON | Asks for an object such as `{"response":{"key":"value"}}`, with NULL for missing values. |
| `ai_fix_grammar` | VARCHAR | Corrected text. |
| `ai_redact` | VARCHAR | Text with detected values replaced by category markers such as `[EMAIL]`. |
| `ai_translate` | VARCHAR | Translated text. See language and NULL rules below. |
| `ai_similarity` | FLOAT | A model-generated semantic similarity score from 0 through 1, not a vector-distance calculation. |
| `ai_summarize` | VARCHAR | A text summary. |
| `ai_filter` | BOOLEAN | Whether the model considers the text to satisfy the condition. |

Classification and extraction require JSON objects, but do not validate the requested object schema or factual accuracy. All results are nullable and non-deterministic. Row failures follow `ai_function_on_error`; the [SQL restrictions and runtime controls](ai_complete.md#limitations-and-security) also apply.

## Examples

```sql
EXPLAIN SELECT ai_sentiment('The delivery was excellent.');
EXPLAIN SELECT ai_classify('Please reset my password.', ['support', 'sales']);
EXPLAIN SELECT ai_translate('Hello', '', 'zh');
EXPLAIN SELECT ai_translate('support_chat', 'chat-model', 'Bonjour', 'fr', 'en');
EXPLAIN SELECT ai_summarize('chat-model', 'A local test paragraph.');
```
