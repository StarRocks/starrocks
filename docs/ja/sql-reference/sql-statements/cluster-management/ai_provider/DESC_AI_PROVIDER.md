---
displayed_sidebar: docs
description: "1 つの AI プロバイダーの完全な設定を表示します。"
---

# DESC AI PROVIDER

1 つの AI プロバイダーの完全な設定を、すべてのプロパティを含めて `Name` / `Value` の行として表示します。
`api_key` の値はマスクされます。

`DESCRIBE` は `DESC` の同義語として使用できます。

## 構文

```SQL
{ DESC | DESCRIBE } AI PROVIDER <provider_name>
```

## パラメータ

| パラメータ      | 説明                           |
| --------------- | ------------------------------ |
| `provider_name` | 表示するプロバイダーの名前。     |

## 例

```sql
DESC AI PROVIDER openai;
```

## 関連する SQL ステートメント

- [`SHOW AI PROVIDERS`](./SHOW_AI_PROVIDERS.md)
- [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)
