---
displayed_sidebar: docs
description: "将外部 AI 服务 provider（embedding 或 rerank）注册为 SQL 管理的集群元数据。"
---

# CREATE AI PROVIDER

在集群中注册一个外部 AI 服务 provider。统一的 AI provider 注册表持有不同**类型（type）**的 provider，
并按**类型各自保留一个默认值**：

- `embedding`——OpenAI 兼容的 `/v1/embeddings` 端点，供 semantic-context 模块在 `CONTEXT UPSERT`
  写入和 `query_text` 检索时计算 embedding。
- `rerank`——Cohere 兼容的 `/rerank` 端点（Cohere / Jina / Voyage / OpenRouter / 本地 TEI），供
  `/api/context/search` 可选的 cross-encoder 第二阶段重排使用。
- `text`——为将来的文本生成 / 推理 provider 预留。

provider 对象（含 `api_key` 属性）持久化在 FE 元数据 journal 与 image 中，重启和升级后凭证仍保留。
这些设置没有 `fe.conf` 开关。

> **注意**
>
> 任何能读取 FE meta image / BDB journal 目录的人都能看到 `api_key`；请用文件系统权限保护这些文件。

## 语法

```SQL
CREATE AI PROVIDER [IF NOT EXISTS] <provider_name>
TYPE { embedding | rerank | text }
[ COMMENT '<comment>' ]
PROPERTIES (
    "endpoint"   = "<url>",
    "model"      = "<model_name>"
    [, "dimensions" = "<int>" ]      -- 仅 embedding
    [, "max_documents" = "<int>" ]   -- 仅 rerank（可选）
    [, "deadline_ms" = "<int>" ]     -- 仅 rerank（可选）
    [, "timeout_ms" = "<int>" ]
    [, "api_key" = "<key>" ]
)
```

## 参数

| 参数            | 说明                                                                                                |
| --------------- | --------------------------------------------------------------------------------------------------- |
| `provider_name` | provider 名称。用于 `SET ... AS DEFAULT AI PROVIDER`，并显示在 `SHOW AI PROVIDERS` 中。              |
| `TYPE`          | `embedding`、`rerank` 或 `text`。决定允许哪些属性，以及该 provider 可成为哪个类型的默认值。          |
| `COMMENT`       | 可选注释。                                                                                          |
| `PROPERTIES`    | `"key" = "value"` 形式的配置。允许的 key 取决于 `TYPE`（见下）。其它 key 会被拒绝。                  |

### PROPERTIES

| 属性            | 适用类型       | 必填 | 说明                                                                                 |
| --------------- | -------------- | ---- | ------------------------------------------------------------------------------------ |
| `endpoint`      | 全部           | 是   | HTTP(S) 端点 URL，必须以 `http://` 或 `https://` 开头。                               |
| `model`         | 全部           | 是   | 请求体 `model` 字段的模型名（如 `text-embedding-3-small`、`cohere/rerank-4-fast`）。 |
| `dimensions`    | embedding      | 否   | embedding 向量维度（正整数）。必须与 provider 输出及向量索引维度一致。                |
| `max_documents` | rerank         | 否   | 每次 rerank 请求发送的最大文档数（正整数；默认 1000）。                               |
| `deadline_ms`   | rerank         | 否   | 整个 rerank 调用（含所有重试）的总时间预算（毫秒，正整数；默认 10000）。用于限制慢/不可达的 rerank 服务最多拖慢搜索多久,超过后降级为融合排序。超时不重试,仅连接失败或 HTTP 5xx 才重试。 |
| `timeout_ms`    | 全部           | 否   | 单次请求 HTTP 超时（毫秒，正整数）。对 rerank 而言,实际会被剩余的 `deadline_ms` 预算进一步限制。 |
| `api_key`       | 全部           | 否   | `Authorization` 头的 Bearer token。本地免鉴权 provider 可省略。                       |

## 示例

注册一个 embedding provider 并设为 `embedding` 类型的默认：

```sql
CREATE AI PROVIDER openai TYPE embedding
PROPERTIES (
    "endpoint"   = "https://api.openai.com/v1/embeddings",
    "model"      = "text-embedding-3-small",
    "dimensions" = "1536",
    "api_key"    = "sk-..."
);
SET openai AS DEFAULT AI PROVIDER;   -- 成为默认 embedding provider
```

注册一个 rerank provider 并设为 `rerank` 类型的默认（与 embedding 默认相互独立）：

```sql
CREATE AI PROVIDER cohere_rerank TYPE rerank
PROPERTIES (
    "endpoint"   = "https://openrouter.ai/api/v1/rerank",
    "model"      = "cohere/rerank-4-fast",
    "timeout_ms" = "15000",
    "api_key"    = "sk-or-..."
);
SET cohere_rerank AS DEFAULT AI PROVIDER;   -- 成为默认 rerank provider
```

## 相关文档

- [`ALTER AI PROVIDER`](./ALTER_AI_PROVIDER.md)——修改已有 provider 的属性。
- [`DROP AI PROVIDER`](./DROP_AI_PROVIDER.md)——删除 provider。
- [`SHOW AI PROVIDERS`](./SHOW_AI_PROVIDERS.md)——列出 provider（可按 `TYPE` 过滤）。
- [`DESC AI PROVIDER`](./DESC_AI_PROVIDER.md)——查看单个 provider 的完整配置。
- [`SET DEFAULT AI PROVIDER`](./SET_DEFAULT_AI_PROVIDER.md)——设置某类型的默认 provider。
