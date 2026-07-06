---
displayed_sidebar: docs
description: "Register an external AI service provider (embedding or rerank) as SQL-managed cluster metadata."
---

# CREATE AI PROVIDER

Registers an external AI service provider in the cluster. A single, unified AI-provider registry
holds providers of different **types** and keeps one default **per type**:

- `embedding` — OpenAI-compatible `/v1/embeddings` endpoint, used by the semantic-context module to
  embed `CONTEXT UPSERT` content and `query_text` searches.
- `rerank` — Cohere-compatible `/rerank` endpoint (Cohere / Jina / Voyage / OpenRouter / local TEI),
  used by the optional cross-encoder second phase of `/api/context/search`.
- `text` — reserved for future text-generation / reasoning providers.

The provider object — including the `api_key` property — is persisted on the FE metadata journal and
image, so cluster restarts and upgrades retain the credential. There is no `fe.conf` knob for these
settings.

> **CAUTION**
>
> Anyone with read access to the FE meta image / BDB journal directory can read provider `api_key`
> values; protect those files with filesystem permissions.

## Syntax

```SQL
CREATE AI PROVIDER [IF NOT EXISTS] <provider_name>
TYPE { embedding | rerank | text }
[ COMMENT '<comment>' ]
PROPERTIES (
    "endpoint"   = "<url>",
    "model"      = "<model_name>"
    [, "dimensions" = "<int>" ]      -- embedding only
    [, "max_documents" = "<int>" ]   -- rerank only (optional)
    [, "deadline_ms" = "<int>" ]     -- rerank only (optional)
    [, "timeout_ms" = "<int>" ]
    [, "api_key" = "<key>" ]
)
```

## Parameters

| Parameter       | Description                                                                                          |
| --------------- | ---------------------------------------------------------------------------------------------------- |
| `provider_name` | Name of the provider. Used by `SET ... AS DEFAULT AI PROVIDER` and shown in `SHOW AI PROVIDERS`.     |
| `TYPE`          | `embedding`, `rerank`, or `text`. Determines which properties are allowed and which per-type default this provider can become. |
| `COMMENT`       | Optional comment string.                                                                             |
| `PROPERTIES`    | `"key" = "value"` configuration. Allowed keys depend on `TYPE` (see below). Any other key is rejected. |

### PROPERTIES

| Property        | Types          | Required | Description                                                                                          |
| --------------- | -------------- | -------- | ---------------------------------------------------------------------------------------------------- |
| `endpoint`      | all            | Yes      | HTTP(S) endpoint URL. Must start with `http://` or `https://`.                                       |
| `model`         | all            | Yes      | Model name passed in the request `model` field (e.g. `text-embedding-3-small`, `cohere/rerank-4-fast`). |
| `dimensions`    | embedding      | No       | Embedding vector dimension (positive int). Must match the provider output and the vector index dim. |
| `max_documents` | rerank         | No       | Max documents sent per rerank request (positive int; default 1000).                                  |
| `deadline_ms`   | rerank         | No       | Overall wall-clock budget in ms for the whole rerank call across all retry attempts (positive int; default 10000). Bounds how long a slow/unreachable reranker can stall a search before it degrades to fusion order. A timeout is never retried; only a connection failure or HTTP 5xx is. |
| `timeout_ms`    | all            | No       | Per-request HTTP timeout in milliseconds (positive int). For rerank, effectively capped by the remaining `deadline_ms` budget. |
| `api_key`       | all            | No       | Bearer token for the `Authorization` header. Omit for local providers that don't require auth.       |

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

Register a rerank provider and make it the default for the `rerank` type (independent of the
embedding default):

```sql
CREATE AI PROVIDER cohere_rerank TYPE rerank
PROPERTIES (
    "endpoint"   = "https://openrouter.ai/api/v1/rerank",
    "model"      = "cohere/rerank-4-fast",
    "timeout_ms" = "15000",
    "api_key"    = "sk-or-..."
);
SET cohere_rerank AS DEFAULT AI PROVIDER;   -- becomes the default rerank provider
```

## See also

- [`ALTER AI PROVIDER`](./ALTER_AI_PROVIDER.md) — change an existing provider's properties.
- [`DROP AI PROVIDER`](./DROP_AI_PROVIDER.md) — remove a provider.
- [`SHOW AI PROVIDERS`](./SHOW_AI_PROVIDERS.md) — list providers (optionally filtered by `TYPE`).
- [`DESC AI PROVIDER`](./DESC_AI_PROVIDER.md) — show one provider's full configuration.
- [`SET DEFAULT AI PROVIDER`](./SET_DEFAULT_AI_PROVIDER.md) — set the default provider for its type.
