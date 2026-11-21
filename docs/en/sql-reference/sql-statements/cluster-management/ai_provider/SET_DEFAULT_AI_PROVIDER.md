---
displayed_sidebar: docs
description: "Set an AI provider as the default for its type."
products: [oss, enterprise, cloud, anywhere]
---

# SET DEFAULT AI PROVIDER

Marks a provider as the default for **its own type**. The registry keeps one default per type
(`embedding` / `chat`), so setting an embedding provider as default does not affect the
chat default and vice versa.

`provider` and `model` must be constant VARCHAR expressions. If `provider` is omitted, an empty string, or SQL NULL, the cluster default Provider for the function type is used: `chat` for completion and text helpers, `embedding` for embeddings. If `model` is omitted, an empty string, or SQL NULL, the selected Provider's default model is used; an explicit model takes precedence. Values containing only whitespace are invalid. An explicit Provider call does not require a cluster default.

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
SET dashscope_chat AS DEFAULT AI PROVIDER;  -- default chat provider
```

## See also

- [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)
- [`SHOW AI PROVIDERS`](./SHOW_AI_PROVIDERS.md)
