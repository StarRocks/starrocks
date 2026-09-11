---
displayed_sidebar: docs
description: "Set an AI provider as the default for its type."
---

# SET DEFAULT AI PROVIDER

Marks a provider as the default for **its own type**. The registry keeps one default per type
(`embedding` / `rerank` / `text`), so setting an embedding provider as default does not affect the
rerank default and vice versa.

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
