---
displayed_sidebar: docs
description: "Call an OpenAI chat model through an AI Provider."
---

# ai_complete

Calls an OpenAI chat-completions endpoint through an AI Provider and returns generated text. The prompt, model, options, and any FILE content are sent to the selected endpoint.

Both `USAGE ON AI FUNCTION <function_name>` and `USAGE ON AI PROVIDER <provider_name>` are required, including for the selected default Provider. See [AI function privileges](ai_functions.mdx#ai-function-privileges).

## Syntax

```sql
ai_complete(prompt)
ai_complete(prompt, options)
ai_complete(prompt, file)
ai_complete(prompt, file, options)
ai_complete(model, prompt)
ai_complete(model, prompt, options)
ai_complete(model, prompt, file)
ai_complete(model, prompt, file, options)
ai_complete(provider, model, prompt)
ai_complete(provider, model, prompt, options)
ai_complete(provider, model, prompt, file)
ai_complete(provider, model, prompt, file, options)
```

## Parameters

Omitting `provider` selects the cluster default Provider for the function type: `chat` for completion and text helpers, `embedding` for embeddings. Omitting `model` uses that Provider's required default model. An explicit model takes precedence and may vary by row. `provider` must be a nonblank constant string; a constant explicit model must not be blank. An explicit Provider call does not require a cluster default.

NULL text, explicit model, or FILE input returns NULL without a model request, whether it is a constant or a runtime column value. A NULL options MAP means no options; a NULL translation source language requests automatic detection. Provider names and helper arrays retain their parameter validation. Nested NULL values inside options retain their JSON-compatible semantics. If an untyped NULL matches different argument types across overloads, specify its type with `CAST`, for example `CAST(NULL AS VARCHAR)` or `CAST(NULL AS MAP<VARCHAR, JSON>)`.

### Options MAP rules

`options` is an optional constant MAP. Omit it or use `map{}` for no options. Keys at every MAP depth must be unique, non-NULL, nonempty VARCHAR values. Values may be NULL, BOOLEAN, finite numbers, strings, JSON, ARRAY, MAP, or STRUCT. Unsupported value types are rejected. The case-sensitive top-level keys `model`, `endpoint`, `key`, `api_key`, `protocol`, `provider`, `messages`, and `stream` are reserved. Connection settings and credentials come from the Provider.

### FILE inputs

FILE overloads accept typed inline JPEG, PNG, or WebP images for OpenAI chat. The model must support image input. Video, audio, and documents are unsupported. FILE is not implicitly converted from a URL or VARCHAR, and no URI is fetched. Only `inline` may carry content; `uri`, `offset`, and `size` must be NULL. The MIME type is detected from bytes, and a supplied `content_type` must match it.

`ai_function_max_input_file_bytes` limits raw bytes before copying and Base64 encoding; its default is 10485760 bytes (10 MiB). Invalid or oversized inline data follows `ai_function_on_error`. Reference FILE inputs fail with `NotSupported`, including with `on_error=ignore`. Runtime NULL FILE values return NULL without a request.

## Return value

Returns nullable VARCHAR from `choices[0].message.content`. Results are non-deterministic. `ai_function_on_error` controls row-level failures: `ignore` returns NULL (the default), and `fail` aborts the query. Analysis/configuration errors, cancellation, and deadlines are not ignored.

## Configuration

Create a Provider with `protocol = "openai"`. Both HTTP and HTTPS endpoints are supported, with or without an API key. A nonempty key produces an `Authorization: Bearer ...` header. URL format, control-character, DNS, and address validation still apply. Use a trusted endpoint and data approved for transmission.

Set a default with `SET <provider_name> AS DEFAULT AI PROVIDER`. If the corresponding default is absent, FE rejects implicit Provider calls, including `EXPLAIN`. Each execution plan captures its selected Provider configuration. ALTER and default changes affect newly planned queries; each prepared `EXECUTE` replans against current metadata.

### Runtime limits and retries

- [`ai_function_rate_limit_qps_chat`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_chat)
  and
  [`ai_function_max_inflight`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_inflight)
  apply independently on each BE. The QPS limit is maintained per endpoint, credential, and capability bucket; the
  in-flight limit is process-wide. WorkGroup- and query-aware admission shares the applicable limits among queued
  requests. Every initial or retry HTTP attempt must obtain both admission permits.
- StarRocks cannot guarantee exactly-once execution at the model provider. A timed-out or otherwise failed attempt may
  already have reached the provider, so a retry can repeat provider work and incur additional charges. Configure
  [`ai_function_max_retries`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_retries)
  and
  [`ai_function_max_retries_on_throttle`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_retries_on_throttle)
  according to the provider's behavior and billing policy.
- Request and response payloads are charged to the query memory tracker, and the execution pipeline applies
  backpressure while requests are outstanding.
  [`ai_function_max_response_bytes`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_response_bytes)
  is a hard limit for each HTTP response body.
- The independent task timeout is fixed for the task and does not restart on retry. Query cancellation and deadline
  updates are observed throughout asynchronous execution. Timeout, worker, and scheduling-granularity controls are
  documented with the other
  [BE AI function parameters](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_request_timeout_ms).

## Limitations and security

- `ai_complete` cannot be used in `GROUP BY`, `SELECT DISTINCT`, aggregate-function arguments, or window-function
  expressions.
- It cannot be used inside `IF`, `IFNULL`, `NULLIF`, `COALESCE`, or `CASE` conditional expressions, or as a table
  function argument.
- It cannot be used in materialized-view definitions or generated-column expressions.
- It cannot be used in a lambda expression body or a SQL UDF body.
- SQL plan baselines cannot be created or bound for statements that contain `ai_complete`; queries that contain it
  are planned without SPM rewrite.
- `PREPARE` is supported, but a statement that contains `ai_complete` is fully replanned for every `EXECUTE`; its
  execution plan is not reused.
- Within a correlated query block, AI expressions are rejected from the `SELECT` list, `WHERE`, `HAVING`,
  `ORDER BY`, and `JOIN ON`, even when the AI expression itself references only local columns. AI expressions in join
  conditions are supported only for `INNER JOIN` and `CROSS JOIN`.
- Each non-NULL input row can produce a remote request. Account for network latency, provider quotas, query deadlines,
  cost, and data-egress policy before using the function over many rows.

## Examples

```sql
CREATE AI PROVIDER support_chat TYPE chat
PROPERTIES ("endpoint" = "http://localhost:8000/v1/chat/completions",
            "model" = "chat-model", "protocol" = "openai");
SET support_chat AS DEFAULT AI PROVIDER;

EXPLAIN SELECT ai_complete('Summarize this text.');
EXPLAIN SELECT ai_complete('another-model', 'Summarize this text.');
EXPLAIN SELECT ai_complete('support_chat', 'another-model', 'Summarize this text.', map{});
EXPLAIN SELECT ai_complete('support_chat', 'vision-model', 'Describe this image.', image_file)
FROM media_inputs;
EXPLAIN SELECT ai_complete('support_chat', 'another-model', CAST(NULL AS VARCHAR));
EXPLAIN SELECT ai_complete('another-model', 'text', CAST(NULL AS MAP<VARCHAR, JSON>));
```
