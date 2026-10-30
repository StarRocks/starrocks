---
displayed_sidebar: docs
description: "Use AI functions to generate, embed, classify, extract, translate, summarize, and evaluate text."
sidebar_position: 21
toc_max_heading_level: 2
---

# AI text functions

This page describes all AI functions for text processing. Before calling them, configure an AI Provider and obtain usage privileges on both the function and the selected Provider. See the [AI functions overview](ai_functions.mdx).

| Function | Purpose | Return type |
| --- | --- | --- |
| [ai_complete](#ai_complete) | Generate text from a prompt | VARCHAR |
| [ai_embed](#ai_embed) | Generate a text embedding | `ARRAY<FLOAT>` |
| [ai_sentiment](#ai_sentiment) | Analyze text sentiment | VARCHAR |
| [ai_classify](#ai_classify) | Classify text into given categories | JSON |
| [ai_extract](#ai_extract) | Extract specified information | JSON |
| [ai_fix_grammar](#ai_fix_grammar) | Correct grammar and spelling | VARCHAR |
| [ai_redact](#ai_redact) | Redact sensitive information by category | VARCHAR |
| [ai_translate](#ai_translate) | Translate text | VARCHAR |
| [ai_similarity](#ai_similarity) | Evaluate the semantic similarity of two texts | FLOAT |
| [ai_summarize](#ai_summarize) | Generate a summary | VARCHAR |
| [ai_filter](#ai_filter) | Determine whether text meets a condition | BOOLEAN |

## Common calling rules

### Passing arguments

Functions support positional arguments and named arguments in the form `argument_name => value`. Named arguments can appear in any order and allow you to specify optional arguments without supplying defaults for earlier arguments.

```sql
-- Use the default chat AI Provider and its default model.
SELECT ai_complete('Explain columnar storage.');

-- Positional arguments: override the default model of chat_provider with chat-model.
SELECT ai_complete('Explain columnar storage.', 'chat_provider', 'chat-model');

-- Named arguments: override the default model of the default chat AI Provider.
SELECT ai_complete(prompt => 'Explain columnar storage.', model => 'chat-model');
```

In these examples, `chat_provider` refers to an existing chat AI Provider.

### Provider and model

All functions have optional `provider` and `model` arguments. Both must be constant VARCHAR expressions.

| Argument | When omitted, an empty string, or SQL NULL | When explicitly specified |
| --- | --- | --- |
| `provider` | Use the cluster default Provider of the corresponding type | Use the named Provider; no cluster default is required |
| `model` | Use the selected Provider's default model | Use the specified model offered by the selected service |

`ai_embed` requires an embedding Provider. All other functions require a chat Provider.

Provider configuration includes an endpoint and optional credentials, and supports HTTP and HTTPS. Each execution plan captures a snapshot of the selected Provider's configuration. Changing a Provider or switching the default affects newly planned queries. For management syntax, see [CREATE AI PROVIDER](../../sql-statements/cluster-management/ai_provider/CREATE_AI_PROVIDER.md) and [SET DEFAULT AI PROVIDER](../../sql-statements/cluster-management/ai_provider/SET_DEFAULT_AI_PROVIDER.md).

### JSON options {/* #json-options */}

Only `ai_complete` and `ai_embed` support the optional `options` argument. It must be a constant JSON expression, and any value other than SQL NULL must be a JSON object. When omitted, it defaults to an empty object. Both `json_object()` and SQL NULL mean that no additional options are passed.

```sql
SELECT ai_complete(
    prompt => 'Explain columnar storage in one sentence.',
    options => json_object('temperature', 0.2)
);
```

Use options to pass additional request parameters supported by the model service.

## ai_complete

Generates text from a prompt using the chat completion capability of a chat Provider.

### Syntax

```sql
ai_complete(prompt[, provider[, model[, options]]])
```

### Arguments

- `prompt`: A VARCHAR expression describing the task for the model. You can use a text column.
- `options`: An optional constant JSON object. See [JSON options](#json-options).

For the types and default behavior of `provider` and `model`, see [Common calling rules](#common-calling-rules).

### Return value

Returns VARCHAR containing the text in `choices[0].message.content` from the response body.

### Examples

```sql
SELECT ai_complete('Explain what columnar storage is in one sentence.');

SELECT ai_complete(
    prompt => 'Write a slogan for a bookstore.',
    provider => 'support_chat',
    model => 'chat-model',
    options => json_object('temperature', 0.2)
);
```

## ai_embed

Converts text into an embedding using an embedding Provider. URL strings are treated as plain text; the content at the URL is not fetched.

### Syntax

```sql
ai_embed(input[, provider[, model[, options]]])
```

### Arguments

- `input`: A VARCHAR expression containing the text to embed.
- `options`: An optional constant JSON object. `dimensions` defaults to the Provider configuration and can be overridden through options. The model and service must support the requested dimensions.

For the types and default behavior of `provider` and `model`, see [Common calling rules](#common-calling-rules).

### Return value

Returns a nullable `ARRAY<FLOAT>`. The service response must contain one nonempty embedding at index 0, with finite FLOAT values. Invalid responses follow the [error handling](#error-handling) rules.

### Examples

```sql
SELECT ai_embed('Columnar storage is suitable for analytical queries.');

SELECT ai_embed(
    input => 'Columnar storage is suitable for analytical queries.',
    provider => 'text_embedding',
    model => 'embedding-model',
    options => json_object('dimensions', 256)
);
```

## ai_sentiment

Analyzes the overall sentiment of text.

### Syntax

```sql
ai_sentiment(input[, provider[, model]])
```

### Arguments

- `input`: A VARCHAR expression containing the text to analyze.

For the types and default behavior of `provider` and `model`, see [Common calling rules](#common-calling-rules).

### Return value

Returns a nullable VARCHAR. Valid results are normalized to one of `positive`, `negative`, `neutral`, `mixed`, or `unknown`. Other responses are treated as row-level errors.

### Examples

```sql
SELECT ai_sentiment('The delivery was excellent.');

SELECT ai_sentiment(input => 'The delivery was excellent.', provider => 'support_chat');
```

## ai_classify

Asks the model to classify text into one of the given categories.

### Syntax

```sql
ai_classify(input, categories[, provider[, model]])
```

### Arguments

- `input`: A VARCHAR expression containing the text to classify.
- `categories`: A nonempty constant `ARRAY<VARCHAR>` containing the candidate categories.

For the types and default behavior of `provider` and `model`, see [Common calling rules](#common-calling-rules).

### Return value

Returns a nullable JSON object. The requested structure is `{"labels":["selected category"]}`, for example, `{"labels":["support"]}`.

StarRocks checks that the result is a JSON object, but does not validate the number of labels, category membership, or classification accuracy.

### Examples

```sql
SELECT ai_classify('Please reset my password.', ['support', 'sales']);

SELECT ai_classify(
    input => 'Please reset my password.',
    categories => ['support', 'sales'],
    provider => 'support_chat'
);
```

## ai_extract

Extracts information from text using the given field names.

### Syntax

```sql
ai_extract(input, labels[, provider[, model]])
```

### Arguments

- `input`: A VARCHAR expression containing the text to extract information from.
- `labels`: A nonempty constant `ARRAY<VARCHAR>` containing the field names to extract.

For the types and default behavior of `provider` and `model`, see [Common calling rules](#common-calling-rules).

### Return value

Returns a nullable JSON object. The requested structure is `{"response":{"field name":"value"}}`, with JSON null requested for fields that are not found. Example result: `{"response":{"name":"Alice","city":"Paris"}}`.

StarRocks checks that the result is a JSON object, but does not validate the field structure of response or the accuracy of the extracted content.

### Examples

```sql
SELECT ai_extract('Alice lives in Paris.', ['name', 'city']);

SELECT ai_extract(
    input => 'Alice lives in Paris.',
    labels => ['name', 'city'],
    provider => 'support_chat'
);
```

## ai_fix_grammar

Corrects grammar and spelling, asking the model to preserve the original meaning and tone.

### Syntax

```sql
ai_fix_grammar(input[, provider[, model]])
```

### Arguments

- `input`: A VARCHAR expression containing the text to correct.

For the types and default behavior of `provider` and `model`, see [Common calling rules](#common-calling-rules).

### Return value

Returns a nullable VARCHAR containing the corrected text.

### Examples

```sql
SELECT ai_fix_grammar('She go to school every day.');

SELECT ai_fix_grammar(input => 'She go to school every day.', provider => 'support_chat');
```

## ai_redact

Asks the model to identify sensitive information in the specified categories and replace it with category markers.

### Syntax

```sql
ai_redact(input, categories[, provider[, model]])
```

### Arguments

- `input`: A VARCHAR expression containing the original text to process.
- `categories`: A nonempty constant `ARRAY<VARCHAR>` containing the categories to redact, such as `name`, `email`, and `phone`.

For the types and default behavior of `provider` and `model`, see [Common calling rules](#common-calling-rules).

### Return value

Returns a nullable VARCHAR. The model is instructed to replace identified information with the uppercase category name in square brackets, such as `[EMAIL]`, and return the original text if no matching information is found.

The original text is sent to the model service. This function does not redact data locally before transmission, and the model may miss sensitive information.

### Examples

```sql
SELECT ai_redact('Contact Alice at alice@example.com.', ['name', 'email']);

SELECT ai_redact(
    input => 'Contact Alice at alice@example.com.',
    categories => ['name', 'email'],
    provider => 'support_chat'
);
```

## ai_translate

Translates text from the source language into the target language.

### Syntax

```sql
ai_translate(input, source_language, target_language[, provider[, model]])
```

### Arguments

- `input`: A VARCHAR expression containing the text to translate.
- `source_language`: A VARCHAR expression specifying the source language. Use an empty string `''` to request automatic detection.
- `target_language`: A VARCHAR expression specifying the target language.

You can use identifiers such as `en`, `zh`, and `fr`, or language names the model understands. StarRocks does not define a fixed list of supported languages.

For the types and default behavior of `provider` and `model`, see [Common calling rules](#common-calling-rules).

### Return value

Returns a nullable VARCHAR containing the translated text. Returns NULL if the input text is NULL at runtime. A runtime NULL source language is treated as automatic detection. A runtime NULL, empty, or ASCII-whitespace-only target language returns NULL without sending a request.

All three semantic arguments are required and cannot be bare NULL literals. To detect the source language automatically, explicitly pass an empty string.

### Examples

```sql
SELECT ai_translate('Hello', '', 'zh');

SELECT ai_translate(
    input => 'Bonjour',
    source_language => 'fr',
    target_language => 'en',
    provider => 'support_chat'
);
```

## ai_similarity

Asks a chat model to evaluate the semantic similarity of two texts.

### Syntax

```sql
ai_similarity(input, other[, provider[, model]])
```

### Arguments

- `input`: A VARCHAR expression containing the first text.
- `other`: A VARCHAR expression containing the second text.

For the types and default behavior of `provider` and `model`, see [Common calling rules](#common-calling-rules).

### Return value

Returns a nullable FLOAT in the range `[0, 1]`. A higher value indicates greater semantic similarity according to the model. Responses that cannot be parsed or are outside this range are treated as row-level errors.

The score is generated by a chat model; it is not a distance calculation between embedding vectors.

### Examples

```sql
SELECT ai_similarity('A cat is sleeping.', 'A kitten is taking a nap.');

SELECT ai_similarity(
    input => 'A cat is sleeping.',
    other => 'A kitten is taking a nap.',
    provider => 'support_chat'
);
```

## ai_summarize

Asks the model to generate a short summary that preserves the main information.

### Syntax

```sql
ai_summarize(input[, provider[, model]])
```

### Arguments

- `input`: A VARCHAR expression containing the text to summarize.

For the types and default behavior of `provider` and `model`, see [Common calling rules](#common-calling-rules).

### Return value

Returns a nullable VARCHAR containing the summary generated by the model.

### Examples

```sql
SELECT ai_summarize('The meeting concluded that API testing must be completed before the new version is released. The test lead will report progress on Friday.');

SELECT ai_summarize(
    input => 'The meeting concluded that API testing must be completed before the new version is released. The test lead will report progress on Friday.',
    provider => 'support_chat'
);
```

## ai_filter

Determines whether text meets a condition expressed in natural language.

### Syntax

```sql
ai_filter(input, condition[, provider[, model]])
```

### Arguments

- `input`: A VARCHAR expression containing the text to evaluate.
- `condition`: A VARCHAR expression describing the condition in natural language.

For the types and default behavior of `provider` and `model`, see [Common calling rules](#common-calling-rules).

### Return value

Returns a nullable BOOLEAN. The model response must normalize to `true` or `false`. Other responses are treated as row-level errors.

### Examples

```sql
SELECT ai_filter('The package arrived broken.', 'The customer reports damaged goods.');

SELECT ai_filter(
    input => 'The package arrived broken.',
    condition => 'The customer reports damaged goods.',
    provider => 'support_chat'
);
```

## Error handling

`ai_function_on_error` controls row-level failures. The default, `ignore`, returns NULL for a failed row and continues the query; `fail` aborts the query. Analysis or configuration errors, query cancellation, and deadline expiration are not ignored. A NULL result therefore does not necessarily mean the input was NULL; it can also indicate a failed model call or response validation.

## Runtime limits and retries

- Text generation and helper functions use [`ai_function_rate_limit_qps_chat`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_chat),
  while embedding functions use [`ai_function_rate_limit_qps_embedding`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_embedding). Their respective QPS limits
  and
  [`ai_function_max_inflight`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_inflight)
  apply independently on each BE. QPS limits are maintained in buckets keyed by endpoint, credential, and capability, while the in-flight limit is process-wide. Fair admission based on WorkGroup and Query shares the applicable limits among queued requests. Every initial or retry HTTP attempt must acquire both kinds of admission permits.
- StarRocks cannot guarantee that a model provider executes a request only once. An attempt that times out or otherwise fails may already have reached the provider, so retries can repeat work at the provider and incur additional charges. Configure
  [`ai_function_max_retries`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_retries)
  and
  [`ai_function_max_retries_on_throttle`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_retries_on_throttle) according to the provider's behavior and billing policy.
- Request and response payloads are accounted for in the Query memory tracker. The execution Pipeline applies backpressure while requests are outstanding.
  [`ai_function_max_response_bytes`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_response_bytes)
  is a hard limit for each HTTP response body.
- The independent task timeout remains fixed after task creation and does not restart on retries. Query cancellation and deadline updates remain effective throughout asynchronous execution. For timeout, worker thread, and scheduling granularity controls, see the other
  [BE AI function configuration items](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_request_timeout_ms).

For request, retry, and timeout metrics, see [AI HTTP metrics](../../../administration/management/monitoring/metric_details/a-c.md#ai_http_requests_total).

## SQL usage restrictions

- AI functions cannot be used in `GROUP BY`, `SELECT DISTINCT`, aggregate function arguments, or window function expressions.
- They cannot be used in `IF`, `IFNULL`, `NULLIF`, `COALESCE`, or `CASE` conditional expressions, or as table function arguments.
- They cannot be used in synchronous materialized view definitions, LOAD expressions, or generated column expressions.
- They cannot be used in Lambda expression bodies or SQL UDF bodies.
- SQL Plan Baselines cannot be created or bound for statements containing AI functions. Other queries containing AI functions do not use SPM rewrites either.
- `PREPARE` is supported, but statements containing AI functions are fully replanned on every `EXECUTE`; execution plans are not reused.
- In a correlated subquery block, AI expressions in the `SELECT` list, `WHERE`, `HAVING`, `ORDER BY`, and `JOIN ON`
  are rejected even if the AI expression references only columns in that block. AI expressions in join conditions are supported only for `INNER JOIN` and `CROSS JOIN`.
- Each non-NULL input row may produce a remote request. Before using AI functions on many rows, consider network latency, provider quotas, query deadlines, costs, and data egress policies.
