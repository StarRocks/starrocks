---
displayed_sidebar: docs
description: "将 chat 或 embedding 类型的外部服务注册为集群中的 AI Provider。"
products: [oss, enterprise, cloud, anywhere]
---

# CREATE AI PROVIDER

在集群中注册一个外部 AI 服务 provider。统一的 AI provider 注册表持有不同**类型（type）**的 provider，
并按**类型各自保留一个默认值**：

- `embedding`——为 [ai_embed](../../../sql-functions/ai-functions/ai_embed.md) 提供向量端点。semantic-context 模块在 `CONTEXT UPSERT` 写入和 `query_text` 检索时使用 OpenAI 兼容的 `/v1/embeddings` 端点。
- `chat`——为 [ai_complete](../../../sql-functions/ai-functions/ai_complete.md) 等函数提供聊天生成端点。

provider 对象（含 `api_key` 属性）持久化在 FE 元数据 journal 与 image 中，重启和升级后凭证仍保留。
这些设置没有 `fe.conf` 开关。

> **注意**
>
> 任何能读取 FE meta image / BDB journal 目录的人都能看到 `api_key`；请用文件系统权限保护这些文件。

SQL AI 函数使用的 `openai` 和 `dashscope` Provider 均支持 HTTP 和 HTTPS，API key 可选。非空 key 生成 `Authorization: Bearer ...` 请求头。URL 格式、控制字符、DNS 和地址校验仍然生效。请使用可信端点和允许向外传输的数据。

使用 `SET <provider_name> AS DEFAULT AI PROVIDER` 设置默认值。若对应默认 Provider 未设置，隐式调用在 FE 报错，`EXPLAIN` 也会报错。每个执行计划固定所选 Provider 的配置快照；ALTER 和默认值切换影响重新规划的查询，预处理语句每次 `EXECUTE` 都根据当前元数据重新规划。

## 权限

创建或管理 Provider 需要 SYSTEM `OPERATE` 权限。通过 AI 函数调用 Provider，同时需要相应的 [AI 函数权限](../../../sql-functions/ai-functions/ai_functions.mdx#ai-function-privileges)和该 AI PROVIDER 的 `USAGE` 权限。创建 Provider 不会自动授予普通用户使用权限。

## 语法

```SQL
CREATE AI PROVIDER [IF NOT EXISTS] <provider_name>
TYPE { embedding | chat }
[ COMMENT '<comment>' ]
PROPERTIES (
    "endpoint"   = "<url>",
    "model"      = "<model_name>"
    [, "protocol" = "{ openai | dashscope }" ]
    [, "dimensions" = "<int>" ]      -- 仅 embedding
    [, "timeout_ms" = "<int>" ]
    [, "api_key" = "<key>" ]
)
```

## 参数

| 参数            | 说明                                                                                                |
| --------------- | --------------------------------------------------------------------------------------------------- |
| `provider_name` | provider 名称。用于 `SET ... AS DEFAULT AI PROVIDER`，并显示在 `SHOW AI PROVIDERS` 中。              |
| `TYPE`          | `embedding` 或 `chat`。决定允许哪些属性，以及该 provider 可成为哪个类型的默认值。          |
| `COMMENT`       | 可选注释。                                                                                          |
| `PROPERTIES`    | `"key" = "value"` 形式的配置。允许的 key 取决于 `TYPE`（见下）。其它 key 会被拒绝。                  |

### PROPERTIES

| 属性            | 适用类型       | 必填 | 说明                                                                                 |
| --------------- | -------------- | ---- | ------------------------------------------------------------------------------------ |
| `endpoint`      | 全部           | 是   | HTTP(S) 端点 URL，必须以 `http://` 或 `https://` 开头。                               |
| `model`         | 全部           | 是   | 请求体 `model` 字段的模型名（如 `text-embedding-3-small`）。 |
| `protocol`      | 全部           | 否   | SQL AI 函数支持 OpenAI 兼容协议（`openai`）和阿里云百炼原生协议（`dashscope`）。`chat` 和 `embedding` 默认使用 `openai`。 |
| `dimensions`    | embedding      | 否   | embedding 向量维度（正整数）。必须与 provider 输出及向量索引维度一致。                |
| `timeout_ms`    | 全部           | 否   | 单次 HTTP 请求的超时时间，单位为毫秒，取值为正整数。 |
| `api_key`       | 全部           | 否   | `Authorization` 头的 Bearer token。本地免鉴权 provider 可省略。                       |

### SQL AI 函数的端点

百炼提供两种接口。对 `/compatible-mode/v1/...` 兼容端点使用 `openai`，对原生端点使用 `dashscope`。

使用 `dashscope` 时，将 `endpoint` 设为对应操作的完整原生接口 URL，路径须与 Provider 的 `TYPE` 匹配。在 SQL 调用中覆盖 `model` 后，请求仍发送到配置的端点。

| Provider 类型 | 用途 | 原生端点路径 |
| --- | --- | --- |
| `chat` | 文本生成 | `/api/v1/services/aigc/text-generation/generation` |
| `chat` | 多模态生成 | `/api/v1/services/aigc/multimodal-generation/generation` |
| `embedding` | 文本向量 | `/api/v1/services/embeddings/text-embedding/text-embedding` |
| `embedding` | 多模态向量 | `/api/v1/services/embeddings/multimodal-embedding/multimodal-embedding` |

各端点可接受的输入见[媒体支持表](../../../sql-functions/ai-functions/ai_functions.mdx#file-input)。多模态向量模型的格式限制见[百炼多模态向量 API](https://help.aliyun.com/zh/model-studio/multimodal-embedding-api-reference)。

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

下面的示例使用百炼原生多模态生成端点创建一个 `chat` Provider。

```sql
CREATE AI PROVIDER dashscope_chat TYPE chat
PROPERTIES (
    "protocol" = "dashscope",
    "endpoint" = "https://dashscope.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation",
    "model"    = "qwen3-vl-plus",
    "api_key"  = "YOUR_DASHSCOPE_API_KEY"
);
```

## 相关文档

- [`ALTER AI PROVIDER`](./ALTER_AI_PROVIDER.md)——修改已有 provider 的属性。
- [`DROP AI PROVIDER`](./DROP_AI_PROVIDER.md)——删除 provider。
- [`SHOW AI PROVIDERS`](./SHOW_AI_PROVIDERS.md)——列出 provider（可按 `TYPE` 过滤）。
- [`DESC AI PROVIDER`](./DESC_AI_PROVIDER.md)——查看单个 provider 的完整配置。
- [`SET DEFAULT AI PROVIDER`](./SET_DEFAULT_AI_PROVIDER.md)——设置某类型的默认 provider。
