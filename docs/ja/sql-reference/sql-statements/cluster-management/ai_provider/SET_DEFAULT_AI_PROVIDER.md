---
displayed_sidebar: docs
description: "AI プロバイダーをそのタイプのデフォルトとして設定します。"
---

# SET DEFAULT AI PROVIDER

プロバイダーを**そのプロバイダー自身のタイプ**のデフォルトとして設定します。レジストリはタイプごと
(`embedding` / `rerank` / `text`) に 1 つのデフォルトを保持するため、embedding プロバイダーをデフォルトに
設定しても rerank のデフォルトには影響せず、その逆も同様です。

## 構文

```SQL
SET <provider_name> AS DEFAULT AI PROVIDER
```

## パラメータ

| パラメータ      | 説明                                           |
| --------------- | ---------------------------------------------- |
| `provider_name` | デフォルトに設定する既存プロバイダーの名前。      |

## 例

```sql
SET openai AS DEFAULT AI PROVIDER;          -- default embedding provider
SET cohere_rerank AS DEFAULT AI PROVIDER;   -- default rerank provider
```

## 関連する SQL ステートメント

- [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)
- [`SHOW AI PROVIDERS`](./SHOW_AI_PROVIDERS.md)
