---
displayed_sidebar: docs
description: "创建、管理和授权集群级 AI 模型，对接外部聊天与文本向量服务。"
sidebar_position: 23
---

# AI 模型

AI 模型是独立的集群级对象，不属于 Resource，也不隶属 Database 或 Catalog。名称区分大小写，用于标识经批准的外部推理服务，而非存储在 StarRocks 中的训练模型。对象保存能力、提供商、完整端点、提供商模型名称和 BE 本地凭证引用，不保存 API key。

创建 AI 模型或使用新函数前，必须升级所有 FE 和 BE。旧节点无法理解新的日志操作、权限与执行元数据。不支持混合版本使用，也不支持创建这些元数据后的降级；仅删除模型不能清除其日志和权限历史。

:::warning
推理会将输入数据发送到集群外，并可能产生费用。CREATE AI MODEL 可以选择运维预先配置的凭证引用，只应授予可信管理员。CREATE 和 EXPLAIN 均不会访问模型提供商。
:::

## 创建与管理

```sql
CREATE AI MODEL [IF NOT EXISTS] model_name
[COMMENT 'description']
PROPERTIES (
    "capability" = "CHAT",
    "provider" = "openai_compatible",
    "endpoint" = "https://models.example.com/v1/chat/completions",
    "model" = "approved-chat-model",
    "credential_ref" = "SUPPORT"
);

ALTER AI MODEL [IF EXISTS] model_name SET ("model" = "approved-chat-model-v2");
ALTER AI MODEL [IF EXISTS] model_name COMMENT = 'Updated description';
SHOW AI MODELS [LIKE 'pattern'];
DESC AI MODEL model_name;
DROP AI MODEL [IF EXISTS] model_name;
```

方括号表示可选语法。model_name 使用不带数据库前缀的标识符，例如 `support_chat`。CREATE 必须提供以上五个属性。未知属性（包括 `type` 和原始 `api_key`）会被拒绝。不支持 CREATE OR REPLACE 和 RENAME。

| 属性 | 要求 |
|------|------|
| `capability` | 必须为 `CHAT` 或 `TEXT_EMBEDDING`，创建后不可修改。 |
| `provider` | `openai_compatible`。 |
| `endpoint` | 含主机的完整 HTTPS POST URL；显式端口为 1–65535。不能含用户信息、查询串、片段或控制字符。不会自动追加路径。 |
| `model` | 非空白的提供商模型名称，不能含控制字符。 |
| `credential_ref` | `[A-Z0-9_]{1,64}`，创建后不可修改。它是公开引用，不是密钥。 |

ALTER 完整校验后才发布新的不可变修订。失败时原模型不变，重复设置相同值不产生新修订。修改端点后，BE 本地凭证绑定也必须匹配，新查询才能执行。更换能力或凭证引用需要创建新模型。

SHOW 仅返回调用者可见的模型名称。DESC 返回 Id、Name、Revision、Capability、Provider、Endpoint、Model、CredentialRef、Comment，不保存或返回密钥。

文本向量模型使用 `"capability" = "TEXT_EMBEDDING"` 和完整向量端点，例如 `https://models.example.com/v1/embeddings`。

### BE 本地凭证绑定

对每个引用 `<REF>`，在所有执行查询的 BE 上配置进程环境变量：

- `AI_FUNCTION_CREDENTIAL_<REF>_ENDPOINT`：必须与模型端点完全一致。
- `AI_FUNCTION_CREDENTIAL_<REF>_API_KEY`：对应的 Bearer 凭证。

例如 SUPPORT 使用 `AI_FUNCTION_CREDENTIAL_SUPPORT_ENDPOINT` 和 `AI_FUNCTION_CREDENTIAL_SUPPORT_API_KEY`。只接受固定命名规则，修改后需重启对应 BE。绑定缺失或端点不匹配时执行失败，不会回退到 SYSTEM 凭证。端点 DNS 校验和地址固定沿用 [ai_complete](ai_complete.md#be-本地凭证)。

## 授权

复用已有角色、GRANT/REVOKE、Native 授权和 Ranger 框架。PUBLIC 不会自动获得 USAGE，数据库所有者也不会自动获得模型权限。对已有用户可执行：

```sql
GRANT CREATE AI MODEL ON SYSTEM TO 'model_admin'@'%';
GRANT USAGE ON AI MODEL support_chat TO 'analyst'@'%';
GRANT ALTER, DROP ON AI MODEL support_chat TO 'model_admin'@'%';
GRANT USAGE ON ALL AI MODELS TO 'ai_service'@'%';
REVOKE USAGE ON AI MODEL support_chat FROM 'analyst'@'%';
```

CREATE 要求 SYSTEM 级 CREATE AI MODEL；ALTER、DROP 分别要求模型的对应权限。SHOW/DESC 要求模型的任意权限。函数调用要求 USAGE，包括嵌套查询、普通视图、安全视图和预处理语句的每次执行。安全视图对表权限的处理不能替代调用者的模型 USAGE 权限。

Native 的对象级授权绑定稳定模型 ID；删除并同名重建会生成新 ID，不继承旧对象级授权。通配授权仍然适用。Ranger 使用 `ai_model` 名称资源，因此匹配该名称的策略可以授权重建后的模型。需同步更新 Ranger StarRocks service definition；Ranger 拒绝时不会回退到 Native 授权。

## 调用模型

```sql
ai_custom_query(model_name, text)
ai_custom_query(model_name, text, options)
ai_custom_embedding(model_name, text)
ai_custom_embedding(model_name, text, options)
```

- `model_name`：解析为准确模型名称的非 NULL、非空白常量 VARCHAR 表达式，不是逐行变化的提供商模型名称。
- `text`：VARCHAR 表达式。URL 字符串仍是文本，不支持 FILE、图像或多模态输入。
- `options`：可选常量 MAP，带类型的 NULL MAP 视为空。聊天沿用 [ai_complete 选项规则](ai_complete.md#options-map-规则)，向量沿用 [ai_embed](ai_embed.md#语法)。
- `ai_custom_query` 要求 CHAT，返回可为 NULL 的 VARCHAR；`ai_custom_embedding` 要求 TEXT_EMBEDDING，返回可为 NULL 的 `ARRAY<FLOAT>`。模型缺失或能力不匹配在执行前失败。

NULL 文本不发送提供商请求。沿用已有[错误策略和 SQL 限制](ai_complete.md#限制和安全)，不依赖 SYSTEM 聊天或向量默认配置。

语句引用的所有命名模型在鉴权前一起捕获。鉴权和所有 AIProject 节点共用这些不可变修订。绑定后的 ALTER/DROP 不改变已授权的计划；后续规划读取当前元数据。预处理语句每次执行都会重新规划。撤权或删除不会追溯取消正在执行的查询。

Query dump 不捕获 AI 模型元数据，暂不支持命名模型调用的离线重放；重放不能用当前同名模型替代未捕获的快照。

```sql
EXPLAIN SELECT ai_custom_query('support_chat', 'A local test prompt.');
EXPLAIN SELECT ai_custom_embedding('search_embedding', 'A local test sentence.');
```

EXPLAIN 校验元数据和权限，但不发送 HTTP 请求。已有 SYSTEM `ai_complete`、文本辅助函数和 `ai_embed` 的路由保持不变，不解析 AI 模型对象。
