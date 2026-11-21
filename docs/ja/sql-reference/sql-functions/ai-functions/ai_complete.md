---
displayed_sidebar: docs
description: "プロンプトと、必要に応じて指定した画像または動画からテキストを生成します。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_complete

chat Provider を使用してテキストを生成します。プロンプトのみを渡すことも、画像または動画を 1 つ添えてその内容について質問することもできます。

## 構文

```sql
ai_complete(prompt[, provider[, model[, options]]])
ai_complete(prompt, file[, provider[, model[, options]]])
```

## 引数

- `prompt`: モデルに実行してほしいタスクを記述する VARCHAR 式。テキスト列も使用できます。
- `file`: 画像または動画を 1 つ指定する FILE 式。[FILE 入力の対応範囲](ai_functions.mdx#file-input)を参照してください。
- `options`: オプションの定数 JSON オブジェクト。[JSON options](ai_functions.mdx#json-options) を参照してください。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](ai_functions.mdx#共通の呼び出しルール)を参照してください。

## 戻り値

モデルが生成したテキストを `VARCHAR` として返します。

## 例

```sql
SELECT ai_complete('列指向ストレージとは何かを一文で説明してください。');

SELECT ai_complete(
    prompt => '書店のキャッチコピーを一つ考えてください。',
    provider => 'support_chat',
    model => 'chat-model',
    options => json_object('temperature', 0.2)
);
```

[視覚モデルの例](ai_functions.mdx#vision-chat)では、`vision_chat` を作成して画像を説明します。同じ Model Studio Provider に動画の URL を渡すこともできます。

```sql
SELECT ai_complete(
    prompt => 'この動画で何が起きているか説明してください。',
    file => to_file(named_struct(
        'uri', 'https://help-static-aliyun-doc.aliyuncs.com/file-manage-files/zh-CN/20250107/lbcemt/new+video.mp4'
    )),
    provider => 'vision_chat'
);
```
