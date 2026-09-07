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

### Reducing AI input rows

For an `ORDER BY ... LIMIT` query with a positive `LIMIT` and no `OFFSET`, StarRocks can select the top rows before
evaluating `ai_complete` when every ordering column passes through the AI projection and any optional ordinary Project
unchanged from its input. With [`enable_ai_topn_pushdown`](../../System_variable.md#enable_ai_topn_pushdown) enabled
(the default), [`ai_topn_pushdown_max_global_limit`](../../System_variable.md#ai_topn_pushdown_max_global_limit)
selects the candidate strategy for SQL `LIMIT N`:

- `N` less than or equal to the threshold (default `1000`): an ordinary `FINAL` candidate TopN bounds input to the
  rewritten AI projection to at most `N` rows globally.
- `N` greater than the threshold: an ordinary `PARTIAL` candidate TopN bounds input to that AI projection to at most `N` rows per
  fragment instance, not per BE or pipeline driver. It is not a per-pipeline TopN.

Both strategies retain the original TopN above the AI projection to preserve the requested output order and limit.
Use `EXPLAIN` to check the candidate TopN below the AI projection and the original TopN above it.

The bound applies to the rewritten AI projection, not all AI work in the query. For nested AI calls, a local candidate
TopN can remain between two AI projections, so the inner AI call can still process the original input rows.

This optimization does not move a TopN below an AI call needed for ordering or filtering. It also does not cross
an existing limit on the AI projection or its immediate input, or unsafe projection or predicate barriers.
Partitioned TopN operations and those using `RANK` or `DENSE_RANK` are not eligible. These eligibility checks apply to
both strategies.

The global strategy can trade MPP parallelism across AI execution instances for fewer AI input rows. The local strategy
adds no global gather before AI execution, but ordinary local merging can reduce AI pipeline parallelism within a
fragment instance; AI requests remain asynchronous. Already-gathered input need not become distributed. Neither
strategy guarantees fewer AI input rows, lower provider cost, or lower latency. These row bounds are not HTTP request,
token, or memory limits: separate AI calls and retries can still produce multiple requests per row.

The error policy for executed AI calls is unchanged. Pruned input rows do not execute AI calls, so errors those calls
might have produced are not observed.

## Configuration

### AI TopN pushdown

[`enable_ai_topn_pushdown`](../../System_variable.md#enable_ai_topn_pushdown) is a Boolean session variable with default
`true`; `false` skips the candidate rewrite. [`ai_topn_pushdown_max_global_limit`](../../System_variable.md#ai_topn_pushdown_max_global_limit)
is a `long` session variable with default `1000` and range `[0, 9223372036854775807]`. Setting the threshold to `0`
selects local-only candidate pruning for eligible queries; it does not disable pushdown. The defaults are an initial
policy choice, not a benchmark-derived optimum.

Both variables support `SET` for the current session, `SET GLOBAL` for future sessions, and a statement-level `SET_VAR`
hint, without restarting. They are independent of `cbo_push_down_topn_limit` and do not affect ordinary queries without
an AI projection.

```sql
-- Disable candidate TopN pushdown below AI projections.
SET enable_ai_topn_pushdown = false;
-- Enable hybrid pruning: global for LIMIT <= 1000, local for larger LIMITs.
SET enable_ai_topn_pushdown = true;
SET ai_topn_pushdown_max_global_limit = 1000;
-- Use local-only candidate pruning while pushdown remains enabled.
SET ai_topn_pushdown_max_global_limit = 0;
```

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
