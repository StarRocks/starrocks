---
displayed_sidebar: docs
description: "Set an AI provider as the default for its type."
---

# SET DEFAULT AI PROVIDER

Marks a provider as the default for **its own type**. The registry keeps one default per type
(`embedding` / `rerank` / `chat`), so setting an embedding provider as default does not affect the
rerank default and vice versa.

Omitting `provider` selects the cluster default Provider for the function type: `chat` for completion and text helpers, `embedding` for embeddings. Omitting `model` uses that Provider's required default model. An explicit model takes precedence and may vary by row. `provider` must be a nonblank constant string; a constant explicit model must not be blank. An explicit Provider call does not require a cluster default.

## Syntax

```SQL
SET <provider_name> AS DEFAULT AI PROVIDER
```

## Parameters

| Parameter       | Description                                    |
| --------------- | ---------------------------------------------- |
| `provider_name` | Name of an existing provider to make default.  |

## Examples

```sql
SET openai AS DEFAULT AI PROVIDER;          -- default embedding provider
SET cohere_rerank AS DEFAULT AI PROVIDER;   -- default rerank provider
```

## See also

- [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)
- [`SHOW AI PROVIDERS`](./SHOW_AI_PROVIDERS.md)
