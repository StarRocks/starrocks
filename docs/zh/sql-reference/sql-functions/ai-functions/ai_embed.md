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
- `options`：可选常量 JSON 对象。`dimensions` 默认继承 Provider 配置，也可通过 options 覆盖；模型和服务必须支持请求的维度。

`provider`、`model` 的类型及默认行为见 [公共调用规则](ai_functions.mdx#公共调用规则)。

## 返回值

返回可空的 `ARRAY<FLOAT>`。服务响应必须包含索引为 0 的一个非空向量，其元素均为有限 FLOAT 值；无效响应按 [错误处理](ai_functions.mdx#错误处理)处理。

## 示例

```sql
SELECT ai_embed('列式存储适合分析查询。');

SELECT ai_embed(
    input => '列式存储适合分析查询。',
    provider => 'text_embedding',
    model => 'embedding-model',
    options => json_object('dimensions', 256)
);
```

下面对同一个图片地址使用同一个[多模态向量 Provider](ai_functions.mdx#video-embedding)。第一个结果是 URL 这段文本的向量，第二个结果是该 URL 所指图片的向量。

```sql
WITH media AS (
    SELECT 'https://dashscope.oss-cn-beijing.aliyuncs.com/images/256_1.png' AS url
)
SELECT ai_embed(input => url, provider => 'vision_embedding') AS url_text_embedding,
       ai_embed(input => to_file(named_struct('uri', url)), provider => 'vision_embedding') AS image_embedding
FROM media;
```
