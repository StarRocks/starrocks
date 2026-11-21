---
displayed_sidebar: docs
description: "AI プロバイダーをそのタイプのデフォルトとして設定します。"
products: [oss, enterprise, cloud, anywhere]
---

# SET DEFAULT AI PROVIDER

指定した Provider を、同じタイプの AI 関数のデフォルトに設定します。`chat` と `embedding` のデフォルトは個別に設定できます。

`provider` と `model` は VARCHAR 型の定数式である必要があります。`provider` を省略するか、空文字列または SQL NULL を指定すると、補完とテキスト補助関数は `chat`、埋め込み関数は `embedding` のクラスターデフォルト Provider を使います。`model` を省略するか、空文字列または SQL NULL を指定すると、選択した Provider のデフォルトモデルを使います。明示したモデルが優先されます。空白文字のみを含む値は無効です。明示 Provider の呼び出しにはクラスターデフォルトの設定は不要です。

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
SET openai AS DEFAULT AI PROVIDER;          -- デフォルトの embedding Provider
SET dashscope_chat AS DEFAULT AI PROVIDER;  -- デフォルトの chat Provider
```

## 関連する SQL ステートメント

- [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)
- [`SHOW AI PROVIDERS`](./SHOW_AI_PROVIDERS.md)
