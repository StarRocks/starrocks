---
displayed_sidebar: docs
description: "AI プロバイダーをそのタイプのデフォルトとして設定します。"
---

# SET DEFAULT AI PROVIDER

プロバイダーを**そのプロバイダー自身のタイプ**のデフォルトとして設定します。レジストリはタイプごと
(`embedding` / `rerank` / `chat`) に 1 つのデフォルトを保持するため、embedding プロバイダーをデフォルトに
設定しても rerank のデフォルトには影響せず、その逆も同様です。

`provider` を省略すると、補完とテキスト補助関数は `chat`、埋め込み関数は `embedding` のクラスターデフォルト Provider を使います。`model` を省略すると Provider の必須デフォルトモデルを使います。明示モデルが優先され、行ごとに変更できます。`provider` は空白でない定数文字列である必要があり、定数の明示モデルも空白にはできません。明示 Provider の呼び出しにはクラスターデフォルトの設定は不要です。

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
