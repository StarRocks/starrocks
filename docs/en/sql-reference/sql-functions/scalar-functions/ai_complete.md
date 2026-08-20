---
displayed_sidebar: docs
description: "Calls a configured SYSTEM OpenAI-compatible chat endpoint and returns the generated text."
---

# ai_complete

Calls the administrator-configured SYSTEM chat model and returns its generated text. StarRocks sends a non-streaming
OpenAI-compatible chat-completions request from a BE.

:::warning
This function sends the model name, prompt, and options to the configured endpoint. Use only a trusted endpoint,
which must use HTTPS, and do not include secrets or sensitive data unless the provider is approved to receive them. Calls can
leave the StarRocks cluster, incur provider charges, and be retained under the provider's data-handling policy.
:::

## Syntax

```sql
ai_complete(<prompt>)
ai_complete(<prompt>, <options>)
ai_complete(<model>, <prompt>)
ai_complete(<model>, <prompt>, <options>)
```

## Parameters

- `prompt`: A VARCHAR expression containing the user prompt. An empty string is valid.
- `model`: A VARCHAR expression that selects the model for this call. When omitted, StarRocks uses
  [`ai_default_chat_model`](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_chat_model).
  An explicit model can vary by row, but a constant model cannot be empty or contain only whitespace.
- `options`: An optional constant MAP of additional fields to add to the provider request. A typed NULL MAP is treated
  as an empty MAP.

### Options MAP rules

- The MAP must be constant. Within every top-level or nested MAP, keys must be unique, non-NULL, non-empty VARCHAR
  values.
- Option values must be JSON-compatible: NULL, BOOLEAN, a finite numeric value, a string, JSON, ARRAY, MAP, or STRUCT.
  Keys in nested MAP values must also be VARCHAR.
- The exact, case-sensitive top-level keys `model`, `messages`, and `stream` are reserved and cannot be supplied.
  StarRocks constructs these fields and always sends a non-streaming request.
- A bare NULL in the two-argument form resolves as `ai_complete(<model>, NULL)`. To pass NULL as `options`, cast it to
  a MAP type, for example `CAST(NULL AS MAP<VARCHAR, JSON>)`.

## Return value

Returns a nullable VARCHAR containing `choices[0].message.content` from a successful OpenAI-compatible response.

- If `prompt` is NULL, the function returns NULL without submitting a provider request.
- For an overload with an explicit `model`, the function also returns NULL without submitting a request if `model` is
  NULL.
- The BE configuration
  [`ai_function_on_error`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error)
  controls row-level failures. Its default, `ignore`, returns NULL for the failed row and continues the query. `fail`
  aborts the query instead.
- `ignore` does not suppress analysis or configuration errors, query cancellation, deadlines, or BE shutdown.

The function is non-deterministic. The same arguments can return different text or fail differently as provider state,
model behavior, and runtime conditions change.

## Configuration

### FE SYSTEM model configuration

An administrator configures the SYSTEM model with these mutable FE parameters. The endpoint and provider are required
for every call. The default model is required only for prompt-only overloads.

| Parameter | Default | Requirement |
|-----------|---------|-------------|
| [`ai_default_chat_endpoint`](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_chat_endpoint) | Empty string | Required. A complete HTTPS POST URL for the chat-completions endpoint. |
| [`ai_default_chat_model`](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_chat_model) | Empty string | Required for prompt-only overloads; optional when every call supplies an explicit model. |
| [`ai_default_chat_provider`](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_chat_provider) | Empty string | Required. The only valid value is `openai_compatible`. |

Each query plan captures a snapshot of these values. Dynamic updates apply only to queries analyzed and planned after
the update; plans that have already been constructed retain their captured snapshot.

### BE-local credential

Set `AI_FUNCTION_MODEL_API_KEY` in the local process environment of every BE that can execute AI queries. The BE reads
this value locally and sends it as the Bearer credential. It is not an FE configuration item and is not included in the
query plan. Also set `AI_FUNCTION_MODEL_ENDPOINT` on each BE to exactly the same complete HTTPS URL as
`ai_default_chat_endpoint`. A BE rejects a plan whose endpoint does not match this local binding. It validates every DNS
address, blocks link-local addresses, and pins the validated DNS snapshot for the request. An exact local binding can
intentionally authorize a private-network model endpoint. Changing either BE-local environment variable requires
restarting that BE. Do not put the credential in SQL text or the options MAP.

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

The following examples use `EXPLAIN`, which analyzes and plans the statement without executing the function or sending
an HTTP request. Valid SYSTEM configuration is still required during analysis.

```sql
EXPLAIN SELECT ai_complete('Summarize this local test prompt.');

EXPLAIN SELECT ai_complete(
    'Classify this local test prompt.',
    map{'temperature': 0.0}
);

EXPLAIN SELECT ai_complete(
    'local-test-model',
    'Summarize this local test prompt.'
);

EXPLAIN SELECT ai_complete(
    'local-test-model',
    'Return a JSON object for this local test prompt.',
    map{'response_format': map{'type': 'json_object'}}
);
```

A NULL prompt does not submit a provider request:

```sql
SELECT ai_complete(CAST(NULL AS VARCHAR)) AS answer;
```

## Keywords

AI_COMPLETE, AI, LLM
