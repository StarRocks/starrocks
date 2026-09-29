---
displayed_sidebar: docs
description: "Update the properties of an existing AI provider."
---

# ALTER AI PROVIDER

Updates the `PROPERTIES` of an existing AI provider. Supplied keys are merged into the provider's
current properties (existing keys are overwritten, unmentioned keys are kept). The provider `TYPE`
cannot be changed.

## Syntax

```SQL
ALTER AI PROVIDER [IF EXISTS] <provider_name>
SET ("key" = "value" [, ...])
```

## Parameters

| Parameter       | Description                                                                                     |
| --------------- | ----------------------------------------------------------------------------------------------- |
| `IF EXISTS`     | Do nothing (instead of erroring) when the provider does not exist.                              |
| `provider_name` | Name of the provider to alter.                                                                  |
| `SET (...)`     | `"key" = "value"` pairs to merge. Allowed keys are the same as for the provider's `TYPE` (see [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)). |

## Examples

Rotate the API key and raise the timeout of a provider:

```sql
ALTER AI PROVIDER openai SET (
    "api_key"    = "sk-new-...",
    "timeout_ms" = "20000"
);
```

## See also

- [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)
- [`SHOW AI PROVIDERS`](./SHOW_AI_PROVIDERS.md)
- [`DESC AI PROVIDER`](./DESC_AI_PROVIDER.md)
