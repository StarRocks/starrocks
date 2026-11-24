---
displayed_sidebar: docs
description: "List registered AI providers, optionally filtered by name pattern or type."
products: [oss, enterprise, cloud, anywhere]
---

# SHOW AI PROVIDERS

Lists the registered AI providers, one row per provider. The `api_key` column is always masked.
Results can be filtered by name pattern or by provider type.

## Syntax

```SQL
SHOW AI PROVIDERS [ LIKE '<pattern>' | TYPE { embedding | chat } ]
```

## Parameters

| Parameter        | Description                                                                 |
| ---------------- | --------------------------------------------------------------------------- |
| `LIKE '<pattern>'` | Only show providers whose name matches the SQL `LIKE` pattern.            |
| `TYPE <type>`    | Only show providers of the given type (`embedding` or `chat`).   |

## Return columns

The following columns apply to chat and embedding providers.

| Column         | Description                                                          |
| -------------- | -------------------------------------------------------------------- |
| `Name`         | Provider name.                                                       |
| `Type`         | Provider type.                                                       |
| `Protocol`     | Wire protocol of the endpoint.                                       |
| `IsDefault`    | Whether this provider is the default for its type (`true`/`false`).  |
| `Endpoint`     | Endpoint URL.                                                        |
| `Model`        | Model name.                                                          |
| `Dimensions`   | Embedding dimension (embedding providers).                           |
| `TimeoutMs`    | Per-request HTTP timeout in milliseconds.                            |
| `ApiKey`       | Masked API key.                                                      |
| `Comment`      | Provider comment.                                                    |

## Examples

```sql
SHOW AI PROVIDERS;
SHOW AI PROVIDERS LIKE 'open%';
SHOW AI PROVIDERS TYPE chat;
```

## See also

- [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)
- [`DESC AI PROVIDER`](./DESC_AI_PROVIDER.md)
