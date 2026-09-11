---
displayed_sidebar: docs
description: "将某个 AI provider 设为其类型的默认值。"
---

# SET DEFAULT AI PROVIDER

将某个 provider 设为**其所属类型**的默认值。注册表按类型（`embedding` / `rerank` / `text`）各自保留
一个默认值，因此把一个 embedding provider 设为默认不会影响 rerank 的默认值，反之亦然。

## 语法

```SQL
SET <provider_name> AS DEFAULT AI PROVIDER
```

## 参数

| 参数            | 说明                            |
| --------------- | ------------------------------- |
| `provider_name` | 要设为默认的已有 provider 名称。 |

## 示例

```sql
SET openai AS DEFAULT AI PROVIDER;          -- 默认 embedding provider
SET cohere_rerank AS DEFAULT AI PROVIDER;   -- 默认 rerank provider
```

## 相关文档

- [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)
- [`SHOW AI PROVIDERS`](./SHOW_AI_PROVIDERS.md)
