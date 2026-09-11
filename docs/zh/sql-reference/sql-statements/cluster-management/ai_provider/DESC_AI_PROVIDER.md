---
displayed_sidebar: docs
description: "查看单个 AI provider 的完整配置。"
---

# DESC AI PROVIDER

以 `Name` / `Value` 行的形式显示单个 AI provider 的完整配置，包含全部属性。`api_key` 的值以掩码显示。

`DESCRIBE` 可作为 `DESC` 的同义词。

## 语法

```SQL
{ DESC | DESCRIBE } AI PROVIDER <provider_name>
```

## 参数

| 参数            | 说明                    |
| --------------- | ----------------------- |
| `provider_name` | 要查看的 provider 名称。 |

## 示例

```sql
DESC AI PROVIDER openai;
```

## 相关文档

- [`SHOW AI PROVIDERS`](./SHOW_AI_PROVIDERS.md)
- [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)
