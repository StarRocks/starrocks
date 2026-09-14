---
displayed_sidebar: docs
description: "列出已注册的 AI provider，可按名称模式或类型过滤。"
---

# SHOW AI PROVIDERS

列出已注册的 AI provider，每个 provider 一行。`api_key` 列始终以掩码显示。可按名称模式或类型过滤。

## 语法

```SQL
SHOW AI PROVIDERS [ LIKE '<pattern>' | TYPE { embedding | rerank | text } ]
```

## 参数

| 参数               | 说明                                                        |
| ------------------ | ----------------------------------------------------------- |
| `LIKE '<pattern>'` | 仅显示名称匹配该 SQL `LIKE` 模式的 provider。               |
| `TYPE <type>`      | 仅显示指定类型（`embedding`、`rerank` 或 `text`）的 provider。 |

## 返回列

| 列             | 说明                                              |
| -------------- | ------------------------------------------------- |
| `Name`         | provider 名称。                                   |
| `Type`         | provider 类型（`embedding` / `rerank` / `text`）。 |
| `IsDefault`    | 是否为其类型的默认值（`true`/`false`）。          |
| `Endpoint`     | 端点 URL。                                        |
| `Model`        | 模型名。                                          |
| `Dimensions`   | embedding 维度（embedding provider）。            |
| `MaxDocuments` | 每次 rerank 请求的最大文档数（rerank provider）。 |
| `TimeoutMs`    | 单次请求 HTTP 超时（毫秒）。                      |
| `ApiKey`       | 掩码后的 API key。                                |
| `Comment`      | provider 注释。                                   |

## 示例

```sql
SHOW AI PROVIDERS;
SHOW AI PROVIDERS LIKE 'open%';
SHOW AI PROVIDERS TYPE rerank;
```

## 相关文档

- [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)
- [`DESC AI PROVIDER`](./DESC_AI_PROVIDER.md)
