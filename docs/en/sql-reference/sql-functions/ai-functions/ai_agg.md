---
displayed_sidebar: docs
description: "Aggregate groups of text with instructions using the SYSTEM chat model."
sidebar_position: 24
---

# ai_agg

`ai_agg` produces one nullable VARCHAR result per SQL group by applying an instruction to the group's text. It can appear alongside ordinary aggregates such as `count` and `sum`.

`ai_agg` uses the SYSTEM chat configuration and BE-local credentials described in [ai_complete](ai_complete.md#configuration). Upgrade all FE and BE nodes before using this function.

:::warning
Input text, intermediate summaries, and instructions are sent to the configured model service. One group can require multiple requests and retries, with corresponding provider charges. Apply the same data-handling and endpoint approval policies as other AI functions.
:::

## Syntax

```sql
ai_agg(expr, instruction)
ai_agg(expr, instruction, options)
```

- `expr`: A VARCHAR expression. NULL values are skipped; empty strings and duplicate values are retained. FILE inputs are not supported.
- `instruction`: A constant, non-NULL, nonblank VARCHAR expression describing the desired result.
- `options`: An optional constant MAP with the same [options rules as ai_complete](ai_complete.md#options-map-rules). A typed NULL MAP is empty. `response_format` applies only to the final request, including a group that needs only one request. Other options apply to every request. Options such as `max_tokens` limit each request, not the group's total token usage.

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

Execution requires `USAGE ON AI FUNCTION ai_agg` or `USAGE ON ALL AI FUNCTIONS`. See [AI function privileges](ai_functions.mdx#ai-function-privileges) for the existing authorization and execution-context rules.

Given a table `reviews(category, review_text)` and a configured SYSTEM chat model:

```sql
SELECT category, count(*) AS review_count,
       ai_agg(review_text, 'Summarize the main positive and negative themes.') AS summary
FROM reviews
GROUP BY category;
```

With per-request options:

```sql
SELECT category,
       ai_agg(review_text, 'Summarize the recurring issues.',
              map{'temperature': 0.0}) AS summary
FROM reviews
WHERE review_text IS NOT NULL
GROUP BY category;
```

`EXPLAIN` validates and shows the plan without invoking the model. Query profiles report aggregate group and empty-group counts, leaf requests, rounds, and local reduction-budget failures. Existing AI request, retry, and actual token-usage counters include all submitted rounds, including work performed before a group fails.
