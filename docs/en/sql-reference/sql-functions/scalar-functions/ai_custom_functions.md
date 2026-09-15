---
displayed_sidebar: docs
description: "Call registered AI providers for chat completion and text embeddings."
sidebar_position: 23
---

# AI provider functions

`ai_custom_query` and `ai_custom_embedding` use the existing cluster-wide [AI provider registry](../../sql-statements/cluster-management/ai_provider/CREATE_AI_PROVIDER.md). They select a registered provider by name, not a row-varying remote model name. Upgrade every FE and BE before using these functions.

:::warning
Calls send input data outside the cluster and can incur provider charges. This feature does not add AI-function or provider-object invocation privileges. Provider management still requires SYSTEM OPERATE, and existing table, column, and view checks still apply, but these checks do not provide per-provider call isolation. Allow only trusted query users to use providers approved for their data and cost requirements.
:::

## Syntax

```sql
ai_custom_query(provider_name, text)
ai_custom_query(provider_name, text, options)
ai_custom_embedding(provider_name, text)
ai_custom_embedding(provider_name, text, options)
```

- `provider_name`: A constant, non-NULL, nonblank VARCHAR expression containing the exact, case-sensitive provider name.
- `text`: A VARCHAR expression. A URL remains text; StarRocks does not fetch its contents. FILE, image, and multimodal inputs are not supported.
- `options`: An optional constant MAP. A typed NULL MAP is empty. Chat follows the [ai_complete options rules](ai_complete.md#options-map-rules); embedding follows [ai_embed](ai_embed.md#syntax). Options cannot change the selected provider, endpoint, credential, or remote model.

`ai_custom_query` requires a provider of type `chat` and returns nullable VARCHAR. `ai_custom_embedding` requires type `embedding` and returns nullable `ARRAY<FLOAT>`. Both functions currently require the `openai` protocol. A missing provider, wrong type (including `rerank`), unsupported protocol, or invalid execution configuration fails before a request is sent. These calls do not require SYSTEM chat or embedding defaults and never fall back to SYSTEM credentials.

NULL text returns NULL without a provider request. Row-level failures follow [ai_function_on_error](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error). Configuration errors, cancellation, and deadlines are not converted into NULL. The [SQL placement restrictions and prepared-statement rules](ai_complete.md#limitations-and-security) also apply.

## Provider settings

Register and manage providers with the existing [CREATE](../../sql-statements/cluster-management/ai_provider/CREATE_AI_PROVIDER.md), [ALTER](../../sql-statements/cluster-management/ai_provider/ALTER_AI_PROVIDER.md), [SHOW](../../sql-statements/cluster-management/ai_provider/SHOW_AI_PROVIDERS.md), and [DESC](../../sql-statements/cluster-management/ai_provider/DESC_AI_PROVIDER.md) statements. For these functions:

| Property | Invocation behavior |
|----------|---------------------|
| `endpoint` | Complete OpenAI-compatible chat-completions or embeddings request URL. StarRocks does not append a path. Requests carrying a credential require HTTPS; a provider without an API key can use HTTP(S). URL validation, address restrictions, and DNS pinning still apply. |
| `model` | Remote model name sent in the request. |
| `protocol` | Must be `openai`, the default for `chat` and `embedding`. The registry also accepts `anthropic` and `cohere`, but these functions do not execute those protocols. |
| `api_key` | Optional Bearer credential from provider metadata. If absent, no Authorization header is sent. Named calls do not use BE-local SYSTEM credentials. |
| `dimensions` | For embedding, a default request option. An explicit SQL options entry with the same key overrides it. The provider and model must support the requested value. |
| `timeout_ms` | Per-HTTP-attempt timeout, not a replacement for the logical request budget. Each attempt remains bounded by the remaining logical budget and the live query deadline; retries do not reset the logical budget. |

Provider API keys are stored in the FE metadata journal and image and sent to executing BEs through the existing internal RPC path. Protect metadata files and cluster-network access. SHOW and DESC mask the key; this is not a claim that metadata or internal RPC is encrypted. Do not put credentials in function arguments or options.

## Query snapshots and SYSTEM compatibility

A statement captures the referenced providers together during planning. All its AI execution nodes use those captured values. Later ALTER or DROP does not change an in-flight plan; a subsequent query or prepared EXECUTE captures current metadata. Deleting and recreating a name creates a different provider identity. Dropping a provider does not cancel an in-flight query.

Query dumps do not capture provider metadata. Offline replay of named-provider calls is not supported and must not substitute a live provider with the same name.

The `/api/{db}/{table}/_query_plan` export used by Spark/Flink supports only the existing single-table filter-prune-scan operations, not AI functions. This prevents provider credentials from being exported to clients in executable plans. It is an export capability restriction, not an additional AI-function or provider RBAC permission.

The existing per-type default-provider mechanism is unchanged. Setting a default provider does not reroute SYSTEM `ai_complete`, [text helpers](ai_functions.md), or [ai_embed](ai_embed.md). Their existing overloads, FE configuration, and BE-local credentials remain unchanged; an explicit SYSTEM `model` argument still means a remote model name.

## Examples

After an administrator registers `support_chat` with type `chat` and `search_embedding` with type `embedding`, both using the `openai` protocol, these examples plan queries without sending HTTP requests:

```sql
EXPLAIN SELECT ai_custom_query('support_chat', 'A local test prompt.');
EXPLAIN SELECT ai_custom_query('support_chat', 'A local test prompt.', map{'temperature': 0.0});
EXPLAIN SELECT ai_custom_embedding('search_embedding', 'A local test sentence.');
EXPLAIN SELECT ai_custom_embedding('search_embedding', 'A local test sentence.', map{'dimensions': 128});
```

EXPLAIN validates the selected configuration without invoking the external service. Executing SELECT instead can send one remote request for each non-NULL input row, plus retries.
