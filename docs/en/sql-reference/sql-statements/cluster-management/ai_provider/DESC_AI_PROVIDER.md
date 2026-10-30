---
displayed_sidebar: docs
description: "Show the full configuration of a single AI provider."
---

# DESC AI PROVIDER

Shows the full configuration of a single AI provider as `Name` / `Value` rows, including every
property. The `api_key` value is masked.

`DESCRIBE` is accepted as a synonym for `DESC`.

## Syntax

```SQL
{ DESC | DESCRIBE } AI PROVIDER <provider_name>
```

## Parameters

| Parameter       | Description                    |
| --------------- | ------------------------------ |
| `provider_name` | Name of the provider to show.  |

## Examples

```sql
DESC AI PROVIDER openai;
```

## See also

- [`SHOW AI PROVIDERS`](./SHOW_AI_PROVIDERS.md)
- [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)
