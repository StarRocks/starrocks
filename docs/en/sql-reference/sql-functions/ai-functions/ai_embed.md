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
- `options`: An optional constant JSON object. To override the Provider's `dimensions` setting with the native `dashscope` protocol, use `dimension`, for example `json_object('dimension', 512)`. With the `openai` protocol (including Model Studio's OpenAI-compatible endpoint), use `dimensions`. The model and service must support the requested dimension.

For the types and default behavior of `provider` and `model`, see [Common calling rules](ai_functions.mdx#common-calling-rules).

## Return value

Returns a nullable `ARRAY<FLOAT>`. The service response must include a nonempty vector at index 0. Every element must be a finite FLOAT value. StarRocks treats an invalid response as a [row-level error](ai_functions.mdx#error-handling).

## Examples

```sql
SELECT ai_embed('Columnar storage is suitable for analytical queries.');

-- This example assumes text_embedding uses the openai protocol.
SELECT ai_embed(
    input => 'Columnar storage is suitable for analytical queries.',
    provider => 'text_embedding',
    model => 'embedding-model',
    options => json_object('dimensions', 256)
);
```

Both calls use the [`vision_embedding` Provider](ai_functions.mdx#video-embedding). Pass `url` directly to embed the URL string (`url_text_embedding`), or wrap it in a FILE with `to_file` to embed the image at that URL (`image_embedding`).

```sql
WITH media AS (
    SELECT 'https://dashscope.oss-cn-beijing.aliyuncs.com/images/256_1.png' AS url
)
SELECT ai_embed(input => url, provider => 'vision_embedding') AS url_text_embedding,
       ai_embed(input => to_file(named_struct('uri', url)), provider => 'vision_embedding') AS image_embedding
FROM media;
```
