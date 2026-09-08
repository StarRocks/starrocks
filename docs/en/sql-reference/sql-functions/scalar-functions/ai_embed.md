---
displayed_sidebar: docs
description: "Generate a nullable ARRAY of FLOAT text embedding using a SYSTEM OpenAI-compatible endpoint."
sidebar_position: 22
---

# ai_embed

Generates a text embedding through a SYSTEM OpenAI-compatible embeddings endpoint. The result is a nullable `ARRAY<FLOAT>`, not a Snowflake VECTOR type. Upgrade all FE and BE nodes before using this function.

:::warning
Text, model, and options are sent to the configured provider. Requests can leave the cluster, incur charges, and be retained by the provider. Use only approved endpoints and data.
:::

## Syntax

```sql
ai_embed(text)
ai_embed(text, options)
ai_embed(model, text)
ai_embed(model, text, options)
```

- `text`: A VARCHAR expression. A URL string is embedded as text; StarRocks does not fetch its contents. FILE and image inputs are not supported.
- `model`: An optional VARCHAR expression, which can vary by row. A constant explicit model must not be blank. If omitted, the SYSTEM embedding default model is used.
- `options`: An optional constant MAP of extra provider request fields. A typed NULL MAP is treated as an empty MAP.

The [recursive options MAP rules for ai_complete](ai_complete.md#options-map-rules) apply, except that the exact, case-sensitive reserved top-level keys are `model`, `input`, and `encoding_format`. StarRocks builds those fields and requests numeric embeddings. Values must be JSON-compatible; DATE, BITMAP, and other unsupported value types are rejected.

A bare NULL in the two-argument form resolves as `ai_embed(model, NULL)`. To supply NULL options, use `CAST(NULL AS MAP<VARCHAR, JSON>)`.

## Configuration

Embedding configuration is independent of chat configuration. There is no fallback to the SYSTEM chat endpoint, model, or credential.

| Mutable FE parameter | Default | Requirement |
|----------------------|---------|-------------|
| [ai_default_embedding_endpoint](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_embedding_endpoint) | Empty string | A complete HTTPS POST URL for the embeddings endpoint. |
| [ai_default_embedding_model](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_embedding_model) | Empty string | Required only when the function does not supply a model. |
| [ai_default_embedding_provider](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_embedding_provider) | Empty string | Must be `openai_compatible`. |

Each plan captures its used configuration. FE changes require no FE restart and affect newly analyzed and planned queries; existing plans retain their snapshot.

On every BE that executes embedding queries, configure these local process environment variables:

- `AI_FUNCTION_EMBEDDING_ENDPOINT`: Exactly the same complete URL as the FE endpoint.
- `AI_FUNCTION_EMBEDDING_API_KEY`: The local Bearer credential, never placed in FE configuration, SQL, or the query plan.

Restart each affected BE after changing either environment variable. Missing credentials or an endpoint-binding mismatch cause execution to fail; chat credentials are not substituted.

[ai_function_rate_limit_qps_embedding](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_embedding) defaults to 128 HTTP attempts per second per endpoint, credential, and embedding capability bucket on each BE. Retries also consume admission permits. The process-wide in-flight limit, timeouts, retries, response-size limit, and [other runtime controls](ai_complete.md#runtime-limits-and-retries) are shared with the AI execution framework.

## Return value and limitations

Requires exactly one embedding at index 0 in the response and returns it as a nonempty array of finite FLOAT values. Multiple embeddings, empty arrays, and invalid or out-of-range numbers are rejected. Its length depends on the model and supported provider options; StarRocks does not fix a vector dimension.

NULL text or a NULL explicit model returns NULL without a provider request. Row-level failures follow [ai_function_on_error](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error). Analysis/configuration errors, cancellation, and deadlines are not ignored. The [SQL placement restrictions and prepared-statement replanning rules](ai_complete.md#limitations-and-security) also apply.

## Examples

With valid SYSTEM embedding configuration, these examples plan the query without sending an HTTP request:

```sql
EXPLAIN SELECT ai_embed('A local test sentence.');
EXPLAIN SELECT ai_embed('A local test sentence.', map{'dimensions': 256});
EXPLAIN SELECT ai_embed('approved-embedding-model', 'A local test sentence.');
EXPLAIN SELECT ai_embed(
    'approved-embedding-model', 'A local test sentence.', map{'dimensions': 256}
);
```

The provider and model must support any supplied option, including `dimensions`. To select a named AI model instead of SYSTEM configuration, use [ai_custom_embedding](ai_model.md).
