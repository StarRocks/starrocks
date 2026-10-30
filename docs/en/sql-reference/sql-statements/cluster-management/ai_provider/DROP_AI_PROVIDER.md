---
displayed_sidebar: docs
description: "Remove an AI provider from the cluster."
---

# DROP AI PROVIDER

Removes an AI provider from the cluster.

A provider that is currently the default for its type cannot be dropped — set another provider as the
default for that type first (see [`SET DEFAULT AI PROVIDER`](./SET_DEFAULT_AI_PROVIDER.md)).

## Syntax

```SQL
DROP AI PROVIDER [IF EXISTS] <provider_name>
```

## Parameters

| Parameter       | Description                                                        |
| --------------- | ----------------------------------------------------------------- |
| `IF EXISTS`     | Do nothing (instead of erroring) when the provider does not exist. |
| `provider_name` | Name of the provider to drop.                                     |

## Examples

```sql
DROP AI PROVIDER openai;
DROP AI PROVIDER IF EXISTS cohere_rerank;
```

## See also

- [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)
- [`SET DEFAULT AI PROVIDER`](./SET_DEFAULT_AI_PROVIDER.md)
