---
displayed_sidebar: docs
description: "从集群中删除一个 AI provider。"
---

# DROP AI PROVIDER

从集群中删除一个 AI provider。

当前作为其类型默认值的 provider 不能被删除——请先为该类型设置另一个默认 provider（见
[`SET DEFAULT AI PROVIDER`](./SET_DEFAULT_AI_PROVIDER.md)）。

## 语法

```SQL
DROP AI PROVIDER [IF EXISTS] <provider_name>
```

## 参数

| 参数            | 说明                                   |
| --------------- | -------------------------------------- |
| `IF EXISTS`     | provider 不存在时不报错，直接返回。    |
| `provider_name` | 要删除的 provider 名称。               |

## 示例

```sql
DROP AI PROVIDER openai;
DROP AI PROVIDER IF EXISTS cohere_rerank;
```

## 相关文档

- [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)
- [`SET DEFAULT AI PROVIDER`](./SET_DEFAULT_AI_PROVIDER.md)
