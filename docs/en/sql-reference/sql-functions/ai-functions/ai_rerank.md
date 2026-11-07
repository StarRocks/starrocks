---
displayed_sidebar: docs
description: "Rerank candidate documents for a query using an AI Provider."
sidebar_position: 23
toc_max_heading_level: 2
---

# ai_rerank

Reranks candidate text documents for a query. The query and candidate array can vary for each input row. This function uses a `rerank` AI Provider and the existing asynchronous AI execution framework.

## Syntax

```sql
ai_rerank(query, documents[, provider[, model[, options]]])
```

Use either positional arguments or named arguments (`argument_name => value`) within a call. Do not mix the two forms.

## Arguments

| Argument | Type | Description |
| --- | --- | --- |
| `query` | VARCHAR | Query text; a constant or column expression. |
| `documents` | `ARRAY<VARCHAR>` | Candidate text in its original order; a constant or column expression. |
| `provider` | Constant VARCHAR | Optional Provider name. Omitted, empty, or SQL NULL selects the default `rerank` Provider. |
| `model` | Constant VARCHAR | Optional model override. Omitted, empty, or SQL NULL uses the Provider's default model. |
| `options` | Constant JSON | Optional JSON object. Omitted, `{}`, or SQL NULL passes no extra options. |

`top_n` in `options` must be an exact positive integer no greater than 2147483647. It defaults to the candidate count. All candidates are sent; the result contains `min(top_n, candidate_count)` hits. `top_n` does not reduce input size. Other options are forwarded to the configured service. Options cannot override `model`, `query`, `documents`, `input`, or `parameters`.

## Provider

Supported protocols are `cohere` (the default for `rerank`) and `dashscope_rerank` (native DashScope text reranking). Configure the complete service endpoint; StarRocks does not append a path. For native DashScope, use `https://dashscope.aliyuncs.com/api/v1/services/rerank/text-rerank` and a model supported by that service.

The Provider's `max_documents` rejects rows exceeding the candidate limit without silently truncating them. Its `deadline_ms` limits a row's request across retries and is capped by the query and AI runtime deadlines. `timeout_ms` bounds each attempt. AI functions retain their existing retry and error policies; they do not fall back to the original candidate order.

Calling the function requires both AI function and Provider usage privileges. Configure Providers through [CREATE AI PROVIDER](../../sql-statements/cluster-management/ai_provider/CREATE_AI_PROVIDER.md). Upgrade FE and BE to a version that supports this function before using it.

If `max_documents` is omitted, no Provider-specific candidate limit is imposed. If `deadline_ms` is omitted, the existing AI runtime and query budgets still apply.

## Return value

Returns a nullable `ARRAY<STRUCT<index INT, relevance_score DOUBLE>>`, ordered by non-increasing relevance score. `index` is the original **zero-based** position in `documents`; SQL array subscripts are **one-based**. Equal scores are allowed and retain the service's order. Scores are service-specific and are not probabilities.

- SQL NULL in `query` or `documents` returns SQL NULL without an HTTP request.
- A non-NULL query with an empty candidate array returns `[]` without an HTTP request.
- Empty and duplicate document strings are preserved. A NULL document element is a row-level error.
- Wrong result count, duplicate/out-of-range indices, non-finite scores, or increasing scores are row-level errors. Results are not repaired or reordered.
- `ai_function_on_error = ignore` returns NULL for failed rows; `fail` aborts the query. Invalid configuration/options, cancellation, and local resource failures remain fatal.

## Examples

```sql
CREATE AI PROVIDER reranking TYPE rerank
PROPERTIES (
    "protocol" = "cohere",
    "endpoint" = "https://api.cohere.com/v2/rerank",
    "model" = "YOUR_RERANK_MODEL",
    "api_key" = "YOUR_API_KEY",
    "max_documents" = "1000",
    "deadline_ms" = "10000"
);
SET reranking AS DEFAULT AI PROVIDER;

SELECT ai_rerank(
    query => 'database for real-time analytics',
    documents => ['StarRocks is an analytical database.', 'A recipe for bread.'],
    provider => 'reranking',
    options => json_object('top_n', 1)
);
```

`index` must be quoted when accessing the struct field. Compute the AI function before passing its result to `UNNEST`:

```sql
WITH ranked AS (
    SELECT ai_rerank('database', ['StarRocks', 'bread']) AS hits
)
SELECT hits[1].`index` AS document_index, hits[1].relevance_score AS score
FROM ranked;

WITH ranked AS (
    SELECT ai_rerank('database', ['StarRocks', 'bread']) AS hits
)
SELECT hit.`index`, hit.relevance_score
FROM ranked, UNNEST(hits) AS t(hit);
```

## Admission and usage

`ai_function_rate_limit_qps_rerank` defaults to 4 requests per second per BE admission bucket (endpoint, credential, capability). It applies to attempts including retries, can be updated live, and shares the existing process-wide in-flight limit. See [BE AI configuration](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_rerank).

Only token counts explicitly reported by the service are recorded. Missing fields remain unknown; Cohere search units are not converted into tokens. No token estimates or synthetic totals are introduced.
