---
displayed_sidebar: docs
description: "主要な情報を維持した短い要約を生成するよう、モデルに要求します。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_summarize

主要な情報を維持した短い要約を生成するよう、モデルに要求します。

## 構文

```sql
ai_summarize(input[, provider[, model]])
```

## 引数

- `input`: 要約するテキストを含む VARCHAR 式。

`provider` と `model` の型およびデフォルトの動作については、[共通の呼び出しルール](ai_functions.mdx#共通の呼び出しルール)を参照してください。

## 戻り値

モデルが生成した要約を含む、NULL 許容の VARCHAR を返します。

## 例

```sql
SELECT ai_summarize('会議では、API テストを完了してから新バージョンをリリースすることが決まりました。テスト責任者は金曜日に進捗を報告します。');

SELECT ai_summarize(
    input => '会議では、API テストを完了してから新バージョンをリリースすることが決まりました。テスト責任者は金曜日に進捗を報告します。',
    provider => 'support_chat'
);
```
