---
displayed_sidebar: docs
description: "テキストをソース言語からターゲット言語に翻訳します。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_translate

テキストをソース言語からターゲット言語に翻訳します。

## 構文

```sql
ai_translate(input, source_language, target_language[, provider[, model]])
```

## 引数

- `input`: 翻訳するテキストを含む VARCHAR 式。
- `source_language`: ソース言語を指定する VARCHAR 式。自動検出を要求するには、空文字列 `''` を使用します。
- `target_language`: ターゲット言語を指定する VARCHAR 式。

言語には `en`、`zh`、`fr` などの識別子、またはモデルが理解できる言語名を使用できます。StarRocks では、指定可能な言語の固定リストは定義していません。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](ai_functions.mdx#共通の呼び出しルール)を参照してください。

## 戻り値

翻訳されたテキストを含む、NULL 許容の VARCHAR を返します。入力テキストが実行時に NULL の場合は NULL を返します。ソース言語の実行時 NULL は自動検出として処理します。ターゲット言語が実行時に NULL、空文字列、または ASCII 空白文字のみの場合は、リクエストを送信せずに NULL を返します。

3 つの意味に関わる引数はすべて必須であり、型指定のない NULL リテラルを直接渡すことはできません。ソース言語を自動検出する場合は、空文字列を明示的に渡してください。

## 例

```sql
SELECT ai_translate('Hello', '', 'zh');

SELECT ai_translate(
    input => 'Bonjour',
    source_language => 'fr',
    target_language => 'en',
    provider => 'support_chat'
);
```
