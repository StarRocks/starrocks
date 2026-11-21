---
displayed_sidebar: docs
description: "Generates an embedding for a text string, an image, or a video."
products: [oss, enterprise, cloud, anywhere]
---

# ai_embed

Generates an embedding for one input using an embedding Provider. A VARCHAR input embeds the supplied text. A FILE input embeds its media content.

## Syntax

```sql
ai_embed(input[, provider[, model[, options]]])
```

## Arguments

- `input`: A `VARCHAR` expression for text or a `FILE` expression for one image or video. See [FILE input support](ai_functions.mdx#file-input).
- `options`: An optional constant JSON object. `dimensions` defaults to the Provider configuration and can be overridden through options. The model and service must support the requested dimensions.

For the types and default behavior of `provider` and `model`, see [Common calling rules](ai_functions.mdx#common-calling-rules).

## Return value

Returns a nullable `ARRAY<FLOAT>`. The service response must contain one nonempty embedding at index 0, with finite FLOAT values. Invalid responses follow the [error handling](ai_functions.mdx#error-handling) rules.

## Examples

```sql
SELECT ai_embed('Columnar storage is suitable for analytical queries.');

SELECT ai_embed(
    input => 'Columnar storage is suitable for analytical queries.',
    provider => 'text_embedding',
    model => 'embedding-model',
    options => json_object('dimensions', 256)
);
```

The following query uses the same image URL and the same [multimodal Provider](ai_functions.mdx#video-embedding). The first result represents the URL text. The second represents the image at that URL.

```sql
WITH media AS (
    SELECT 'https://dashscope.oss-cn-beijing.aliyuncs.com/images/256_1.png' AS url
)
SELECT ai_embed(input => url, provider => 'vision_embedding') AS url_text_embedding,
       ai_embed(input => to_file(named_struct('uri', url)), provider => 'vision_embedding') AS image_embedding
FROM media;
```
