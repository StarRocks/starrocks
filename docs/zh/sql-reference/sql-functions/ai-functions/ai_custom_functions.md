---
displayed_sidebar: docs
description: "调用已注册的 AI provider，完成聊天补全和文本向量化。"
sidebar_position: 23
---

# AI provider 函数

`ai_custom_query` 和 `ai_custom_embedding` 复用集群现有的 [AI provider 注册表](../../sql-statements/cluster-management/ai_provider/CREATE_AI_PROVIDER.md)。它们按名称选择已注册的 provider，而不是逐行变化的远端模型名。使用前请升级所有 FE 和 BE。

:::warning
调用会将输入数据发送到集群外，并可能产生提供商费用。请只向查询用户授予已批准的函数族和 Provider。Provider 管理仍要求 SYSTEM OPERATE，已有表、列和视图检查也保持不变。
:::

函数和 Provider 的授权要求见 [AI 函数权限](ai_functions.mdx#ai-function-privileges)。

## 语法

```sql
ai_custom_query(provider_name, text)
ai_custom_query(provider_name, text, options)
ai_custom_embedding(provider_name, text)
ai_custom_embedding(provider_name, text, options)
```

- `provider_name`：非 NULL、非空白的常量 VARCHAR 表达式，内容为准确且区分大小写的 provider 名称。
- `text`：VARCHAR 表达式。URL 仍作为文本处理，StarRocks 不读取其内容。不支持 FILE、图像和多模态输入。
- `options`：可选的常量 MAP，带类型的 NULL MAP 视为空 MAP。聊天遵循 [ai_complete 选项规则](ai_complete.md#options-map-规则)，向量遵循 [ai_embed](ai_embed.md#语法)。选项不能改变所选 provider、端点、凭证或远端模型。

`ai_custom_query` 要求 provider 类型为 `chat`，返回可为 NULL 的 VARCHAR。`ai_custom_embedding` 要求类型为 `embedding`，返回可为 NULL 的 `ARRAY<FLOAT>`。两个函数当前均要求 `openai` 协议。Provider 不存在、类型不匹配（包括 `rerank`）、协议不受支持或执行配置无效时，会在发送请求前失败。这些调用不需要 SYSTEM 聊天或向量默认配置，也不会回退到 SYSTEM 凭证。

文本为 NULL 时直接返回 NULL，不发送提供商请求。行级失败遵循 [ai_function_on_error](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error)。配置错误、取消和截止时间到期不会转换成 NULL。[SQL 使用位置限制和预处理语句规则](ai_complete.md#限制和安全)同样适用。

## Provider 设置

通过现有 [CREATE](../../sql-statements/cluster-management/ai_provider/CREATE_AI_PROVIDER.md)、[ALTER](../../sql-statements/cluster-management/ai_provider/ALTER_AI_PROVIDER.md)、[SHOW](../../sql-statements/cluster-management/ai_provider/SHOW_AI_PROVIDERS.md) 和 [DESC](../../sql-statements/cluster-management/ai_provider/DESC_AI_PROVIDER.md) 语句注册和管理 provider。对于这些函数：

| 属性 | 调用行为 |
|------|----------|
| `endpoint` | 完整的 OpenAI 兼容聊天补全或向量请求 URL。StarRocks 不追加路径。携带凭证的请求必须使用 HTTPS；未配置 API key 的 provider 可使用 HTTP(S)。URL 校验、地址限制和 DNS 固定仍然生效。 |
| `model` | 请求中发送的远端模型名。 |
| `protocol` | 必须为 `openai`，它也是 `chat` 和 `embedding` 的默认协议。注册表也接受 `anthropic` 和 `cohere`，但这些函数不执行这两种协议。 |
| `api_key` | Provider 元数据中可选的 Bearer 凭证。省略时不发送 Authorization 头。具名调用不使用 BE 本地 SYSTEM 凭证。 |
| `dimensions` | 向量请求的默认选项。SQL options 中显式指定的同名键覆盖此值。提供商及模型必须支持请求的值。 |
| `timeout_ms` | 单次 HTTP attempt 的超时，不替代逻辑请求总预算。每次 attempt 仍受剩余逻辑预算和实时 Query deadline 限制；重试不重置逻辑预算。 |

Provider API key 保存在 FE 元数据 journal 和 image 中，并经现有内部 RPC 传给执行请求的 BE。请保护元数据文件和集群网络访问。SHOW 和 DESC 会掩码显示密钥，但这不表示元数据或内部 RPC 已加密。不要把凭证放入函数参数或 options。

## 查询快照与 SYSTEM 兼容性

每个物理执行计划在首次需要某个 provider 时捕获其配置，同一计划中该 provider 的所有调用复用这一快照。权限检查在优化前覆盖所有已解析的 AI 调用。只有保留到物理计划的 AI 调用才捕获执行配置；配置进入计划前，会对实际捕获的 Provider UUID 再次授权检查。后续 ALTER 或 DROP 不改变已捕获的配置，也不会取消正在执行的查询。新构建的计划（包括重试时重新规划或下一次预处理 EXECUTE）会捕获当前元数据。

Query dump 不捕获 provider 元数据。暂不支持具名 provider 调用的离线重放，也不能用当前同名 provider 替代未捕获的快照。

Spark/Flink 使用的 `/api/{db}/{table}/_query_plan` 导出接口仅支持已有的单表过滤、裁剪和扫描（filter-prune-scan），不支持 AI 函数，以避免将可执行计划中的 provider 凭证导出给客户端。这是计划导出的能力限制，不是新增 AI 函数或 provider 的 RBAC 权限。

已有的按类型默认 provider 机制保持不变。设置默认 provider 不会改变 SYSTEM `ai_complete`、[文本辅助函数](ai_text_functions.md) 或 [ai_embed](ai_embed.md) 的路由。它们原有的重载、FE 配置和 BE 本地凭证保持不变；SYSTEM 显式 `model` 参数仍表示远端模型名。

## 示例

管理员已注册 `chat` 类型的 `support_chat` 和 `embedding` 类型的 `search_embedding`，且两者均使用 `openai` 协议后，以下示例只规划查询，不发送 HTTP 请求：

```sql
EXPLAIN SELECT ai_custom_query('support_chat', 'A local test prompt.');
EXPLAIN SELECT ai_custom_query('support_chat', 'A local test prompt.', map{'temperature': 0.0});
EXPLAIN SELECT ai_custom_embedding('search_embedding', 'A local test sentence.');
EXPLAIN SELECT ai_custom_embedding('search_embedding', 'A local test sentence.', map{'dimensions': 128});
```

EXPLAIN 校验所选配置，但不调用外部服务。改为执行 SELECT 后，每个非 NULL 输入行都可能发送一次远程请求，重试会增加请求次数。
