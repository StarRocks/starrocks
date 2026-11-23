---
displayed_sidebar: docs
description: "为一段文本、一张图片或一个视频生成向量。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_embed

使用 embedding Provider 为一个输入生成向量。传入 VARCHAR 时对文本本身做向量化，传入 FILE 时对其中的媒体内容做向量化。

## 语法

```sql
ai_embed(input[, provider[, model[, options]]])
```

## 参数

- `input`：需要向量化的文本（`VARCHAR` 表达式），或一张图片、一个视频（`FILE` 表达式）。参见 [FILE 输入支持范围](ai_functions.mdx#file-input)。
- `options`：可选常量 JSON 对象。要覆盖 Provider 配置的默认维度（`dimensions`），原生 `dashscope` 协议用 `dimension`，例如 `json_object('dimension', 512)`。`openai` 协议（包括百炼的 OpenAI 兼容端点）用 `dimensions`。所选模型和服务必须支持指定的维度。

`provider`、`model` 的类型及默认行为见 [公共调用规则](ai_functions.mdx#公共调用规则)。

## 返回值

返回可空的 `ARRAY<FLOAT>`。服务响应中索引为 0 的向量必须非空，且每个元素都必须是有限的 FLOAT 值。无效响应按[行级错误](ai_functions.mdx#错误处理)处理。

## 示例

```sql
SELECT ai_embed('列式存储适合分析查询。');

-- 本例的 text_embedding 使用 openai 协议。
SELECT ai_embed(
    input => '列式存储适合分析查询。',
    provider => 'text_embedding',
    model => 'embedding-model',
    options => json_object('dimensions', 256)
);
```

两列都使用多模态向量 Provider [`vision_embedding`](ai_functions.mdx#video-embedding)。直接传入 `url` 时，`url_text_embedding` 返回 URL 字符串的向量。用 `to_file` 将这个地址转为 FILE 后，`image_embedding` 返回图片的向量。

```sql
WITH media AS (
    SELECT 'https://dashscope.oss-cn-beijing.aliyuncs.com/images/256_1.png' AS url
)
SELECT ai_embed(input => url, provider => 'vision_embedding') AS url_text_embedding,
       ai_embed(input => to_file(named_struct('uri', url)), provider => 'vision_embedding') AS image_embedding
FROM media;
```
