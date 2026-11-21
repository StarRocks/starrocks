---
displayed_sidebar: docs
description: "テキスト、画像、動画のいずれかから埋め込みベクトルを生成します。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_embed

embedding Provider を使用して、1 つの入力から埋め込みベクトルを生成します。VARCHAR を渡すとテキスト自体を、FILE を渡すとそのメディアの内容をベクトル化します。

## 構文

```sql
ai_embed(input[, provider[, model[, options]]])
```

## 引数

- `input`: ベクトル化するテキストの `VARCHAR` 式、または画像や動画を 1 つ指定する `FILE` 式。[FILE 入力の対応範囲](ai_functions.mdx#file-input)を参照してください。
- `options`: オプションの定数 JSON オブジェクト。`dimensions` はデフォルトで Provider の設定を継承し、options で上書きできます。モデルとサービスが指定された次元数をサポートしている必要があります。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](ai_functions.mdx#共通の呼び出しルール)を参照してください。

## 戻り値

`ARRAY<FLOAT>` または NULL を返します。サービスのレスポンスには、インデックス 0 に空でないベクトルが必要です。各要素は有限の FLOAT 値でなければなりません。不正なレスポンスは[行単位のエラー](ai_functions.mdx#エラー処理)として扱います。

## 例

```sql
SELECT ai_embed('列指向ストレージは分析クエリに適しています。');

SELECT ai_embed(
    input => '列指向ストレージは分析クエリに適しています。',
    provider => 'text_embedding',
    model => 'embedding-model',
    options => json_object('dimensions', 256)
);
```

どちらの列にも同じ Provider [`vision_embedding`](ai_functions.mdx#video-embedding) を使います。`url` をそのまま渡すと、`url_text_embedding` には URL 文字列のベクトルが入ります。同じ URL を `to_file` で FILE にして渡すと、`image_embedding` には画像のベクトルが入ります。

```sql
WITH media AS (
    SELECT 'https://dashscope.oss-cn-beijing.aliyuncs.com/images/256_1.png' AS url
)
SELECT ai_embed(input => url, provider => 'vision_embedding') AS url_text_embedding,
       ai_embed(input => to_file(named_struct('uri', url)), provider => 'vision_embedding') AS image_embedding
FROM media;
```
