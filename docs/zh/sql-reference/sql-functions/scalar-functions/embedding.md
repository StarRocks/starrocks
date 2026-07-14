---
displayed_sidebar: docs
description: "调用兼容 OpenAI 的 embeddings 接口为文本生成嵌入向量，返回 FLOAT 数组。"
---

# embedding

调用兼容 OpenAI 的 `/v1/embeddings` 接口为文本值生成嵌入（embedding）向量，并以 `ARRAY<FLOAT>` 类型返回结果。

该函数在 BE 节点上执行，每行发起一次同步 HTTP 请求。由于计算在 BE 上进行，而非在 FE Leader 上，因此批量导入时的向量生成可以在集群内横向扩展。它主要由语义上下文（AgentBase）模块在写入时用于文本嵌入。参见 [语义上下文](../../../using_starrocks/semantic_context.md)。

## 语法

```SQL
embedding(text, config)
```

## 参数说明

- `text`：待嵌入的文本。类型必须为 `VARCHAR`。
- `config`：Provider 配置。类型必须为 `JSON`。支持以下字段：

| 字段 | 类型 | 是否必填 | 默认值 | 说明 |
|------|------|----------|--------|------|
| `endpoint` | STRING | 是 | - | 兼容 OpenAI 的 embeddings 接口的完整 URL，例如 `https://api.openai.com/v1/embeddings`。 |
| `model` | STRING | 是 | - | 嵌入模型名称，例如 `text-embedding-3-small`。 |
| `api_key` | STRING | 否 | `""` | 用于鉴权的 Bearer Token。当省略或为空时，不发送 `Authorization` 请求头，适用于无需鉴权的本地或自托管 Provider。 |
| `dimensions` | INT | 否 | `0` | 请求的向量维度。当大于 `0` 时，该值会被转发给 Provider，使返回向量与配置的维度一致；当为 `0` 时，使用 Provider 的默认维度。 |
| `timeout_ms` | BIGINT | 否 | `60000` | 单次请求的超时时间，单位为毫秒。 |

## 返回值

返回 `ARRAY<FLOAT>`，即输入文本的嵌入向量。

在以下任一情况下，该行的结果为 `NULL`：

- `text` 参数为 `NULL`，或 `config` 参数为 `NULL`。
- Provider 调用失败（例如接口不可达、超时或返回了非预期的响应），或 Provider 返回了空向量。

某一行失败或为 `NULL` 不会导致整条查询失败，仅该行的值变为 `NULL`。

只有当 `config` 参数格式非法时整条语句才会失败，即：`config` 不是一个 JSON 对象，或缺少必填的 `endpoint`、`model` 字符串字段。

## 使用说明

- 当 `config` 为常量时（常见场景是整条查询传入同一个字面量配置），该配置只会被解析一次，而非逐行解析。
- 每个非 `NULL` 行都会触发一次 HTTP 调用，因此该函数更适合写入时的批量嵌入，而非对延迟敏感的点查询。

## 示例

使用内联配置对字符串字面量进行嵌入：

```SQL
SELECT embedding(
    'StarRocks is a fast analytical database.',
    PARSE_JSON('{
        "endpoint": "https://api.openai.com/v1/embeddings",
        "model": "text-embedding-3-small",
        "api_key": "sk-xxxxxxxx",
        "dimensions": 1536
    }')
);
```

在导入数据时对某一列进行嵌入，并将向量写入目标表：

```SQL
INSERT INTO docs_with_vectors
SELECT
    id,
    content,
    embedding(
        content,
        PARSE_JSON('{
            "endpoint": "https://api.openai.com/v1/embeddings",
            "model": "text-embedding-3-small",
            "dimensions": 1536
        }')
    )
FROM raw_docs;
```

使用无需鉴权的自托管 Provider（省略 `api_key`）：

```SQL
SELECT embedding(
    'hello world',
    PARSE_JSON('{
        "endpoint": "http://127.0.0.1:8080/v1/embeddings",
        "model": "bge-small-en",
        "dimensions": 384
    }')
);
```

## 关键字

EMBEDDING, VECTOR, AI, ARRAY
