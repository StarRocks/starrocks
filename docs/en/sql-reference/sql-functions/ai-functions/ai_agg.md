---
displayed_sidebar: docs
description: "Aggregate groups of text with instructions using a CHAT AI Provider."
sidebar_position: 24
---

# ai_agg

`ai_agg` produces one nullable VARCHAR result per SQL group by applying an instruction to the group's text. It can appear alongside ordinary aggregates such as `count` and `sum`.

`ai_agg` uses a `chat` AI Provider with the `openai` protocol. Endpoint, default model, API key, and optional `timeout_ms` come from that Provider. Upgrade all FE and BE nodes before using this function.

:::warning
Input text, intermediate summaries, and instructions are sent to the configured model service. One group can require multiple requests and retries, with corresponding provider charges. Apply the same data-handling and endpoint approval policies as other AI functions.
:::

## Syntax

```sql
ai_agg(input, instruction[, provider[, model[, options]]])
```

- `input`: A VARCHAR expression. NULL values are skipped; empty strings and duplicate values are retained. FILE inputs are not supported.
- `instruction`: A constant, non-NULL, nonblank VARCHAR expression describing the desired result.
- `provider` and `model`: Optional constant VARCHAR expressions. An omitted, empty, or SQL NULL `provider` selects the default CHAT Provider. The same values for `model` select the Provider's default model; an explicit model overrides it. Whitespace-only selectors are invalid. An explicit Provider needs no cluster default.
- `options`: An optional constant JSON object, following the [JSON options rules](ai_text_functions.md#json-options). Omission or SQL NULL means no additional options. `response_format` applies only to the final request, including a group that needs only one request. Other options apply to every request. `max_tokens` limits each request, not the group's total token usage.

Positional and named arguments are supported, using the [common calling rules](ai_text_functions.md#common-calling-rules).

The function does not support `DISTINCT`, aggregate `ORDER BY`, window execution, or aggregate-state combinators. Apply exact filtering, deduplication, counting, and arithmetic in SQL before AI aggregation when the task requires them. Input order is not guaranteed. Model output is nondeterministic and hierarchical summarization can lose details; the function does not provide exact arithmetic or exhaustive extraction guarantees.

## Return values and errors

An empty global aggregate or an all-NULL group returns NULL without a model request. A grouped query over no rows returns no groups, following ordinary SQL aggregation. Empty strings still participate in the aggregate.

The existing [ai_function_on_error](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error) policy applies to model and reduction-budget failures: `ignore` returns NULL for the **entire group**, while `fail` aborts the query. A failed group never returns a summary of only its successful requests. Configuration errors, invalid aggregate states, the native group-size bound, local resource failures (including scheduling queue exhaustion and memory failures), cancellation, and query deadlines always fail the query. Sanitized local resource failures are reported as a general resource error because their original cause is unavailable; directly detected allocation failures retain the memory-limit error.

The initial implementation uses these fixed safety bounds:

| Bound | Limit |
| --- | --- |
| Encoded input per group | 1 MiB, including a 24-byte header and 8 bytes per non-NULL record |
| Prompt per request | 64 KiB, including the instruction, record escaping, and prompt wrappers |
| Options for intermediate requests | 64 KiB of keys and serialized JSON values, excluding `response_format`; does not apply to a single final request |
| Intermediate summary per request | 4 KiB |
| Retained intermediate summaries per group | 1 MiB across the current and next rounds |
| Rounds per group | 8 |
| Logical requests per group | 1024, excluding retry attempts |

A record that cannot fit a prompt is rejected instead of truncated. Oversized summaries or a reduction that does not shrink sufficiently fail the group. These byte limits are not token estimates and do not guarantee that every provider's context window will accept the request. Existing response-size, rate, in-flight, retry, memory, and query-deadline controls also apply. The request timeout remains per logical request; the query deadline bounds the complete aggregation.

## Privileges and examples

Execution requires `USAGE ON AI FUNCTION ai_agg` (or `USAGE ON ALL AI FUNCTIONS`) and `USAGE ON AI PROVIDER` for the selected CHAT Provider, in addition to table privileges. See [AI function privileges](ai_functions.mdx#ai-function-privileges). Provider configuration and authorization follow the common pre-optimization checks, including empty-input queries.

An administrator registers a [CHAT Provider](../../sql-statements/cluster-management/ai_provider/CREATE_AI_PROVIDER.md), selects the default, and grants access to an existing role:

```sql
CREATE AI PROVIDER review_chat TYPE chat
PROPERTIES (
    "endpoint" = "https://api.example.com/v1/chat/completions",
    "model" = "chat-model",
    "api_key" = "<api-key>"
);
SET review_chat AS DEFAULT AI PROVIDER;
GRANT USAGE ON AI FUNCTION ai_agg TO ROLE review_reader;
GRANT USAGE ON AI PROVIDER review_chat TO ROLE review_reader;
```

The execution plan captures the selected Provider configuration and uses it for every group and reduction round. Metadata and default changes affect newly planned queries. See [CREATE AI PROVIDER](../../sql-statements/cluster-management/ai_provider/CREATE_AI_PROVIDER.md) for credential storage and configuration.

Given a table `reviews(category, review_text)`:

```sql
SELECT category, count(*) AS review_count,
       ai_agg(review_text, 'Summarize the main positive and negative themes.') AS summary
FROM reviews
GROUP BY category;
```

With per-request options:

```sql
SELECT category,
       ai_agg(input => review_text,
              instruction => 'Summarize the recurring issues.',
              provider => 'review_chat',
              options => json_object('temperature', 0.0)) AS summary
FROM reviews
WHERE review_text IS NOT NULL
GROUP BY category;
```

`EXPLAIN` validates and shows the plan without invoking the model. Query profiles report aggregate group and empty-group counts, leaf requests, rounds, and local reduction-budget failures. Existing AI request, retry, and actual token-usage counters include all submitted rounds, including work performed before a group fails.
