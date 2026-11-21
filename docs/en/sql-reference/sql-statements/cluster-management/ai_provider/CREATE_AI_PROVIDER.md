---
displayed_sidebar: docs
description: "Register an external chat or embedding service as an AI Provider in the cluster."
products: [oss, enterprise, cloud, anywhere]
---

# CREATE AI PROVIDER

Registers an external AI service provider in the cluster. A single, unified AI-provider registry
holds providers of different **types** and keeps one default **per type**:

- `embedding` — supplies an endpoint for [ai_embed](../../../sql-functions/ai-functions/ai_embed.md). The semantic-context module uses an OpenAI-compatible `/v1/embeddings` endpoint for `CONTEXT UPSERT` and `query_text` searches.
- `chat` — supplies a chat completion endpoint for [ai_complete](../../../sql-functions/ai-functions/ai_complete.md) and other chat functions.

The provider object — including the `api_key` property — is persisted on the FE metadata journal and
image, so cluster restarts and upgrades retain the credential. There is no `fe.conf` knob for these
settings.

> **CAUTION**
>
> Anyone with read access to the FE meta image / BDB journal directory can read provider `api_key`
> values; protect those files with filesystem permissions.

For SQL AI functions, both `openai` and `dashscope` Providers support HTTP and HTTPS with or without an API key. A nonempty key produces an `Authorization: Bearer ...` header. URL format, control-character, DNS, and address validation still apply. Use a trusted endpoint and data approved for transmission.

Set a default with `SET <provider_name> AS DEFAULT AI PROVIDER`. If the corresponding default is absent, FE rejects implicit Provider calls, including `EXPLAIN`. Each execution plan captures its selected Provider configuration. ALTER and default changes affect newly planned queries; each prepared `EXECUTE` replans against current metadata.

## Privileges

Creating or managing providers requires SYSTEM `OPERATE`. Calling a provider through an AI function requires both the [AI function privilege](../../../sql-functions/ai-functions/ai_functions.mdx#ai-function-privileges) and `USAGE` on that AI PROVIDER. Creating a provider does not automatically grant usage to ordinary users.

## Syntax

```SQL
CREATE AI PROVIDER [IF NOT EXISTS] <provider_name>
TYPE { embedding | chat }
[ COMMENT '<comment>' ]
PROPERTIES (
    "endpoint"   = "<url>",
    "model"      = "<model_name>"
    [, "protocol" = "{ openai | dashscope }" ]
    [, "dimensions" = "<int>" ]      -- embedding only
    [, "timeout_ms" = "<int>" ]
    [, "api_key" = "<key>" ]
)
```

## Parameters

| Parameter       | Description                                                                                          |
| --------------- | ---------------------------------------------------------------------------------------------------- |
| `provider_name` | Name of the provider. Used by `SET ... AS DEFAULT AI PROVIDER` and shown in `SHOW AI PROVIDERS`.     |
| `TYPE`          | `embedding` or `chat`. Determines which properties are allowed and which per-type default this provider can become. |
| `COMMENT`       | Optional comment string.                                                                             |
| `PROPERTIES`    | `"key" = "value"` configuration. Allowed keys depend on `TYPE` (see below). Any other key is rejected. |

### PROPERTIES

| Property        | Types          | Required | Description                                                                                          |
| --------------- | -------------- | -------- | ---------------------------------------------------------------------------------------------------- |
| `endpoint`      | all            | Yes      | HTTP(S) endpoint URL. Must start with `http://` or `https://`.                                       |
| `model`         | all            | Yes      | Model name passed in the request `model` field (e.g. `text-embedding-3-small`). |
| `protocol`      | all            | No       | SQL AI functions support the OpenAI-compatible protocol (`openai`) and the native Alibaba Cloud Model Studio protocol (`dashscope`). The default for `chat` and `embedding` is `openai`. |
| `dimensions`    | embedding      | No       | Embedding vector dimension (positive int). Must match the provider output and the vector index dim. |
| `timeout_ms`    | all            | No       | Per-request HTTP timeout in milliseconds (positive integer). |
| `api_key`       | all            | No       | Bearer token for the `Authorization` header. Omit for local providers that don't require auth.       |

### SQL AI function endpoints

For Alibaba Cloud Model Studio (Bailian), use `openai` with `/compatible-mode/v1/...` endpoints and `dashscope` with native endpoints.

For `dashscope`, set `endpoint` to the full native URL for the operation. Its path must match the Provider `TYPE`. Overriding `model` in SQL still sends requests to the configured endpoint.

| Provider type | Operation | Native endpoint path |
| --- | --- | --- |
| `chat` | Text generation | `/api/v1/services/aigc/text-generation/generation` |
| `chat` | Multimodal generation | `/api/v1/services/aigc/multimodal-generation/generation` |
| `embedding` | Text embedding | `/api/v1/services/embeddings/text-embedding/text-embedding` |
| `embedding` | Multimodal embedding | `/api/v1/services/embeddings/multimodal-embedding/multimodal-embedding` |

See the [supported inputs by endpoint](../../../sql-functions/ai-functions/ai_functions.mdx#file-input) and the [Model Studio multimodal embedding API](https://www.alibabacloud.com/help/en/model-studio/multimodal-embedding-api-reference).

## Examples

Register an embedding provider and make it the default for the `embedding` type:

```sql
CREATE AI PROVIDER openai TYPE embedding
PROPERTIES (
    "endpoint"   = "https://api.openai.com/v1/embeddings",
    "model"      = "text-embedding-3-small",
    "dimensions" = "1536",
    "api_key"    = "sk-..."
);
SET openai AS DEFAULT AI PROVIDER;   -- becomes the default embedding provider
```

The following example registers a `chat` Provider using the native Model Studio multimodal generation endpoint.

```sql
CREATE AI PROVIDER dashscope_chat TYPE chat
PROPERTIES (
    "protocol" = "dashscope",
    "endpoint" = "https://dashscope.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation",
    "model"    = "qwen3-vl-plus",
    "api_key"  = "YOUR_DASHSCOPE_API_KEY"
);
```

## See also

- [`ALTER AI PROVIDER`](./ALTER_AI_PROVIDER.md) — change an existing provider's properties.
- [`DROP AI PROVIDER`](./DROP_AI_PROVIDER.md) — remove a provider.
- [`SHOW AI PROVIDERS`](./SHOW_AI_PROVIDERS.md) — list providers (optionally filtered by `TYPE`).
- [`DESC AI PROVIDER`](./DESC_AI_PROVIDER.md) — show one provider's full configuration.
- [`SET DEFAULT AI PROVIDER`](./SET_DEFAULT_AI_PROVIDER.md) — set the default provider for its type.
