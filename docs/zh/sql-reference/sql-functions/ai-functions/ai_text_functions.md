---
displayed_sidebar: docs
description: "使用 AI 函数完成文本生成、向量化、分类、提取、翻译、摘要与语义判断。"
sidebar_position: 21
toc_max_heading_level: 2
---

# AI 文本类函数

本文介绍所有文本处理 AI 函数。执行前需要配置 AI Provider，并取得函数及所选 Provider 的使用权限，参见 [AI 函数总览](ai_functions.mdx)。

| 函数 | 用途 | 返回类型 |
| --- | --- | --- |
| [ai_complete](#ai_complete) | 按提示词生成文本 | VARCHAR |
| [ai_embed](#ai_embed) | 生成文本向量 | `ARRAY<FLOAT>` |
| [ai_sentiment](#ai_sentiment) | 判断文本情感倾向 | VARCHAR |
| [ai_classify](#ai_classify) | 按给定类别分类 | JSON |
| [ai_extract](#ai_extract) | 提取指定信息 | JSON |
| [ai_fix_grammar](#ai_fix_grammar) | 纠正语法和拼写 | VARCHAR |
| [ai_redact](#ai_redact) | 按类别遮蔽敏感信息 | VARCHAR |
| [ai_translate](#ai_translate) | 翻译文本 | VARCHAR |
| [ai_similarity](#ai_similarity) | 判断两段文本的语义相似度 | FLOAT |
| [ai_summarize](#ai_summarize) | 生成摘要 | VARCHAR |
| [ai_filter](#ai_filter) | 判断文本是否满足条件 | BOOLEAN |

## 公共调用规则

### 参数传递

函数支持位置参数和 `参数名 => 值` 形式的命名参数。命名参数可以调整顺序，也可以直接指定可选参数，无需填写前面的默认值。

```sql
-- 使用 default chat ai provider 和 ai provider 上面绑定的默认模型。
SELECT ai_complete('解释列式存储。');

-- 位置参数：用 chat-model 模型覆盖 chat_provider 上面的默认模型。
SELECT ai_complete('解释列式存储。', 'chat_provider', 'chat-model');

-- 命名参数：覆盖 default chat ai provider 的默认模型。
SELECT ai_complete(prompt => '解释列式存储。', model => 'chat-model');
```

示例中的 `chat_provider` 表示已创建的 chat AI Provider。

### Provider 和模型

所有函数都具有可选的 `provider`、`model` 参数，类型为 VARCHAR，必须是常量表达式。

| 参数 | 省略、空字符串或 SQL NULL 时的行为 | 显式指定时的行为 |
| --- | --- | --- |
| `provider` | 使用对应类型的集群默认 Provider | 使用指定名称的 Provider；无需设置集群默认值 |
| `model` | 使用所选 Provider 的默认模型 | 使用所选服务提供的指定模型 |

`ai_embed` 要求 embedding Provider，其余函数要求 chat Provider。

Provider 配置包含端点和可选凭证，支持 HTTP 和 HTTPS。每个执行计划固定所选 Provider 的配置快照；修改 Provider 或切换默认值影响重新规划的查询。管理语法参见 [CREATE AI PROVIDER](../../sql-statements/cluster-management/ai_provider/CREATE_AI_PROVIDER.md) 和 [SET DEFAULT AI PROVIDER](../../sql-statements/cluster-management/ai_provider/SET_DEFAULT_AI_PROVIDER.md)。

### JSON options {/* #json-options */}

仅 `ai_complete` 和 `ai_embed` 支持可选的 `options`，其类型为 JSON，必须是常量表达式，且非 SQL NULL 的值必须是 JSON 对象。省略时默认为空对象；`json_object()` 和 SQL NULL 都表示不传额外选项。

```sql
SELECT ai_complete(
    prompt => '用一句话解释列式存储。',
    options => json_object('temperature', 0.2)
);
```

options 用于传递模型服务支持的额外请求参数。

## ai_complete

根据提示词生成文本，使用 chat Provider 的聊天补全能力。

### 语法

```sql
ai_complete(prompt[, provider[, model[, options]]])
```

### 参数

- `prompt`：VARCHAR 表达式，描述希望模型完成的任务，可使用文本列。
- `options`：可选常量 JSON 对象，规则见 [JSON options](#json-options)。

`provider`、`model` 的类型及默认行为见 [公共调用规则](#公共调用规则)。

### 返回值

返回 VARCHAR，文本为请求返回体 `choices[0].message.content` 里面的内容。

### 示例

```sql
SELECT ai_complete('用一句话解释什么是列式存储。');

SELECT ai_complete(
    prompt => '为一家书店写一句宣传语。',
    provider => 'support_chat',
    model => 'chat-model',
    options => json_object('temperature', 0.2)
);
```

## ai_embed

将文本转换为向量，使用 embedding Provider。URL 字符串按普通文本处理，不读取 URL 指向的内容。

### 语法

```sql
ai_embed(input[, provider[, model[, options]]])
```

### 参数

- `input`：VARCHAR 表达式，要生成向量的文本。
- `options`：可选常量 JSON 对象。`dimensions` 默认继承 Provider 配置，也可通过 options 覆盖；模型和服务必须支持请求的维度。

`provider`、`model` 的类型及默认行为见 [公共调用规则](#公共调用规则)。

### 返回值

返回可空的 `ARRAY<FLOAT>`。服务响应必须包含索引为 0 的一个非空向量，其元素均为有限 FLOAT 值；无效响应按 [错误处理](#错误处理)处理。

### 示例

```sql
SELECT ai_embed('列式存储适合分析查询。');

SELECT ai_embed(
    input => '列式存储适合分析查询。',
    provider => 'text_embedding',
    model => 'embedding-model',
    options => json_object('dimensions', 256)
);
```

## ai_sentiment

分析文本的整体情感倾向。

### 语法

```sql
ai_sentiment(input[, provider[, model]])
```

### 参数

- `input`：VARCHAR 表达式，要分析的文本。

`provider`、`model` 的类型及默认行为见 [公共调用规则](#公共调用规则)。

### 返回值

返回可空的 VARCHAR。有效结果归一化为 `positive`、`negative`、`neutral`、`mixed` 或 `unknown` 之一；其他响应按行级错误处理。

### 示例

```sql
SELECT ai_sentiment('The delivery was excellent.');

SELECT ai_sentiment(input => 'The delivery was excellent.', provider => 'support_chat');
```

## ai_classify

请求模型将文本归入给定类别中的一个类别。

### 语法

```sql
ai_classify(input, categories[, provider[, model]])
```

### 参数

- `input`：VARCHAR 表达式，要分类的文本。
- `categories`：非空常量 `ARRAY<VARCHAR>`，候选类别。

`provider`、`model` 的类型及默认行为见 [公共调用规则](#公共调用规则)。

### 返回值

返回可空的 JSON 对象。请求的结构为 `{"labels":["选中的类别"]}`，例如 `{"labels":["support"]}`。

StarRocks 校验结果是否为 JSON 对象，但不验证 labels 的数量、类别成员关系或分类准确性。

### 示例

```sql
SELECT ai_classify('Please reset my password.', ['support', 'sales']);

SELECT ai_classify(
    input => 'Please reset my password.',
    categories => ['support', 'sales'],
    provider => 'support_chat'
);
```

## ai_extract

按给定字段名称从文本中提取信息。

### 语法

```sql
ai_extract(input, labels[, provider[, model]])
```

### 参数

- `input`：VARCHAR 表达式，要提取信息的文本。
- `labels`：非空常量 `ARRAY<VARCHAR>`，需要提取的字段名称。

`provider`、`model` 的类型及默认行为见 [公共调用规则](#公共调用规则)。

### 返回值

返回可空的 JSON 对象。请求的结构为 `{"response":{"字段名":"值"}}`，未找到的字段请求返回 JSON null。示意结果：`{"response":{"name":"Alice","city":"Paris"}}`。

StarRocks 校验结果是否为 JSON 对象，但不验证 response 的字段结构或提取内容的准确性。

### 示例

```sql
SELECT ai_extract('Alice lives in Paris.', ['name', 'city']);

SELECT ai_extract(
    input => 'Alice lives in Paris.',
    labels => ['name', 'city'],
    provider => 'support_chat'
);
```

## ai_fix_grammar

纠正文本的语法和拼写，并请求模型保留原意和语气。

### 语法

```sql
ai_fix_grammar(input[, provider[, model]])
```

### 参数

- `input`：VARCHAR 表达式，需要纠正的文本。

`provider`、`model` 的类型及默认行为见 [公共调用规则](#公共调用规则)。

### 返回值

返回可空的 VARCHAR，内容为纠正后的文本。

### 示例

```sql
SELECT ai_fix_grammar('She go to school every day.');

SELECT ai_fix_grammar(input => 'She go to school every day.', provider => 'support_chat');
```

## ai_redact

请求模型识别指定类别的敏感信息，并将其替换为类别标记。

### 语法

```sql
ai_redact(input, categories[, provider[, model]])
```

### 参数

- `input`：VARCHAR 表达式，要处理的原始文本。
- `categories`：非空常量 `ARRAY<VARCHAR>`，需要遮蔽的信息类别，例如 `name`、`email`、`phone`。

`provider`、`model` 的类型及默认行为见 [公共调用规则](#公共调用规则)。

### 返回值

返回可空的 VARCHAR。模型被要求将识别到的信息替换为大写类别名加方括号，例如 `[EMAIL]`；没有匹配信息时返回原始文本。

原始文本会发送给模型服务。此函数不会在数据出站前执行本地脱敏，模型也可能遗漏敏感信息。

### 示例

```sql
SELECT ai_redact('Contact Alice at alice@example.com.', ['name', 'email']);

SELECT ai_redact(
    input => 'Contact Alice at alice@example.com.',
    categories => ['name', 'email'],
    provider => 'support_chat'
);
```

## ai_translate

将文本从源语言翻译为目标语言。

### 语法

```sql
ai_translate(input, source_language, target_language[, provider[, model]])
```

### 参数

- `input`：VARCHAR 表达式，要翻译的文本。
- `source_language`：VARCHAR 表达式，源语言；使用空字符串 `''` 请求自动识别。
- `target_language`：VARCHAR 表达式，目标语言。

语言可使用 `en`、`zh`、`fr` 等标识，或模型理解的语言名称；StarRocks 不提供固定语言白名单。

`provider`、`model` 的类型及默认行为见 [公共调用规则](#公共调用规则)。

### 返回值

返回可空的 VARCHAR，内容为翻译后的文本。输入文本为运行时 NULL 时返回 NULL。源语言的运行时 NULL 按自动识别处理；目标语言的运行时 NULL、空字符串或仅含 ASCII 空白字符时返回 NULL，不发送请求。

三个语义参数均为必填参数，不能直接传裸 NULL。自动识别源语言的调用应显式传空字符串。

### 示例

```sql
SELECT ai_translate('Hello', '', 'zh');

SELECT ai_translate(
    input => 'Bonjour',
    source_language => 'fr',
    target_language => 'en',
    provider => 'support_chat'
);
```

## ai_similarity

请求聊天模型评估两段文本的语义相似度。

### 语法

```sql
ai_similarity(input, other[, provider[, model]])
```

### 参数

- `input`：VARCHAR 表达式，第一段文本。
- `other`：VARCHAR 表达式，第二段文本。

`provider`、`model` 的类型及默认行为见 [公共调用规则](#公共调用规则)。

### 返回值

返回可空的 FLOAT，范围为 `[0, 1]`，数值越大表示模型判断的语义越相近。无法解析或超出范围的响应按行级错误处理。

该分数由聊天模型生成，不是对 embedding 向量进行距离计算。

### 示例

```sql
SELECT ai_similarity('A cat is sleeping.', 'A kitten is taking a nap.');

SELECT ai_similarity(
    input => 'A cat is sleeping.',
    other => 'A kitten is taking a nap.',
    provider => 'support_chat'
);
```

## ai_summarize

请求模型生成保留主要信息的简短摘要。

### 语法

```sql
ai_summarize(input[, provider[, model]])
```

### 参数

- `input`：VARCHAR 表达式，要生成摘要的文本。

`provider`、`model` 的类型及默认行为见 [公共调用规则](#公共调用规则)。

### 返回值

返回可空的 VARCHAR，内容为模型生成的摘要。

### 示例

```sql
SELECT ai_summarize('会议决定先完成接口测试，再发布新版本。测试负责人将在周五汇报进度。');

SELECT ai_summarize(
    input => '会议决定先完成接口测试，再发布新版本。测试负责人将在周五汇报进度。',
    provider => 'support_chat'
);
```

## ai_filter

根据自然语言条件判断文本是否符合要求。

### 语法

```sql
ai_filter(input, condition[, provider[, model]])
```

### 参数

- `input`：VARCHAR 表达式，要判断的文本。
- `condition`：VARCHAR 表达式，用自然语言描述判断条件。

`provider`、`model` 的类型及默认行为见 [公共调用规则](#公共调用规则)。

### 返回值

返回可空的 BOOLEAN。模型响应须能归一化为 `true` 或 `false`，其他响应按行级错误处理。

### 示例

```sql
SELECT ai_filter('The package arrived broken.', 'The customer reports damaged goods.');

SELECT ai_filter(
    input => 'The package arrived broken.',
    condition => 'The customer reports damaged goods.',
    provider => 'support_chat'
);
```

## 错误处理

`ai_function_on_error` 控制行级失败：默认 `ignore` 对失败行返回 NULL 并继续查询；`fail` 终止查询。分析或配置错误、查询取消和截止时间到期不被忽略。返回 NULL 因而不一定表示输入为 NULL，也可能表示模型调用或响应校验失败。

## 运行时限制和重试

- 文本生成及辅助函数使用 [`ai_function_rate_limit_qps_chat`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_chat)，
  向量函数使用 [`ai_function_rate_limit_qps_embedding`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_embedding)。各自的 QPS 限制
  和
  [`ai_function_max_inflight`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_inflight)
  在每个 BE 上独立生效。QPS 限制按 endpoint、credential 和 capability 分桶维护，in-flight 限制为进程级。基于 WorkGroup 和 Query 的公平准入机制会在排队请求之间共享适用的限制。每次初始或重试 HTTP attempt 都必须同时获得这两类准入许可。
- StarRocks 无法保证模型提供商仅执行一次请求。已超时或以其他方式失败的 attempt 可能已经到达提供商，因此重试可能重复提供商侧的工作并产生额外费用。请根据提供商的行为和计费策略配置
  [`ai_function_max_retries`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_retries)
  和
  [`ai_function_max_retries_on_throttle`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_retries_on_throttle)。
- 请求和响应 payload 都会计入 Query 内存跟踪器；存在 outstanding 请求时，执行 Pipeline 会施加背压。
  [`ai_function_max_response_bytes`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_response_bytes)
  是每个 HTTP 响应体的硬上限。
- 独立 task timeout 在 task 创建后保持不变，重试不会重新计时；Query 取消和 deadline 更新会在整个异步执行期间持续生效。超时、工作线程和调度粒度控制参见其他
  [BE AI 函数配置项](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_request_timeout_ms)。

请求数、重试和超时的观测指标参见 [AI HTTP 指标](../../../administration/management/monitoring/metric_details/a-c.md#ai_http_requests_total)。

## SQL 使用限制

- AI 函数不能用于 `GROUP BY`、`SELECT DISTINCT`、聚合函数参数或窗口函数表达式。
- 不能用于 `IF`、`IFNULL`、`NULLIF`、`COALESCE` 或 `CASE` 条件表达式中，也不能作为表函数参数。
- 不能用于同步物化视图定义、LOAD 表达式或生成列表达式。
- 不能用于 Lambda 表达式 body 或 SQL UDF body。
- 包含 AI 函数的语句不能创建或绑定 SQL Plan Baseline；其他包含 AI 函数的查询也不会应用 SPM 改写。
- 支持 `PREPARE`，但包含 AI 函数的语句会在每次 `EXECUTE` 时完整重新规划，不会复用执行计划。
- 在相关子查询的查询块中，`SELECT` 列表、`WHERE`、`HAVING`、`ORDER BY` 和 `JOIN ON` 中的 AI 表达式
  均会被拒绝，即使 AI 表达式自身只引用本层列。联接条件中的 AI 表达式仅支持 `INNER JOIN` 和 `CROSS JOIN`。
- 每个非 NULL 输入行都可能产生一次远程请求。在对大量行使用 AI 函数前，请考虑网络延迟、提供商配额、查询截止时间、费用和数据出站策略。
