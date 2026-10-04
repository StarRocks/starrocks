---
displayed_sidebar: docs
description: "更新已有 AI provider 的属性。"
---

# ALTER AI PROVIDER

更新已有 AI provider 的 `PROPERTIES`。传入的 key 会合并到 provider 当前属性中（同名 key 覆盖，未提及的
key 保留）。provider 的 `TYPE` 不可更改。

## 语法

```SQL
ALTER AI PROVIDER [IF EXISTS] <provider_name>
SET ("key" = "value" [, ...])
```

## 参数

| 参数            | 说明                                                                                            |
| --------------- | ----------------------------------------------------------------------------------------------- |
| `IF EXISTS`     | provider 不存在时不报错，直接返回。                                                             |
| `provider_name` | 要修改的 provider 名称。                                                                        |
| `SET (...)`     | 要合并的 `"key" = "value"` 对。允许的 key 与该 provider 的 `TYPE` 一致（见 [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)）。 |

## 示例

轮换 API key 并提高超时：

```sql
ALTER AI PROVIDER openai SET (
    "api_key"    = "sk-new-...",
    "timeout_ms" = "20000"
);
```

## 相关文档

- [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)
- [`SHOW AI PROVIDERS`](./SHOW_AI_PROVIDERS.md)
- [`DESC AI PROVIDER`](./DESC_AI_PROVIDER.md)
