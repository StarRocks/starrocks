---
displayed_sidebar: docs
description: "指定されたカテゴリの機密情報を識別し、カテゴリを示すマーカーに置き換えるようモデルに要求します。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_redact

指定されたカテゴリの機密情報を識別し、カテゴリを示すマーカーに置き換えるようモデルに要求します。

## 構文

```sql
ai_redact(input, categories[, provider[, model]])
```

## 引数

- `input`: 処理する元のテキストを含む VARCHAR 式。
- `categories`: マスキングする情報のカテゴリを含む、空でない定数 `ARRAY<VARCHAR>`。例えば `name`、`email`、`phone` です。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](ai_functions.mdx#共通の呼び出しルール)を参照してください。

## 戻り値

VARCHAR または NULL を返します。StarRocks は、検出した機密情報を角括弧で囲んだ大文字のカテゴリ名に置き換えるようモデルに指示します。例えば、メールアドレスは `[EMAIL]` に置き換えます。該当する情報がない場合は、元のテキストをそのまま返すよう指示します。

元のテキストはモデルサービスに送信されます。この関数は送信前にローカルでマスキングを行うものではなく、モデルが機密情報を見落とす可能性もあります。

## 例

```sql
SELECT ai_redact('Contact Alice at alice@example.com.', ['name', 'email']);

SELECT ai_redact(
    input => 'Contact Alice at alice@example.com.',
    categories => ['name', 'email'],
    provider => 'support_chat'
);
```
