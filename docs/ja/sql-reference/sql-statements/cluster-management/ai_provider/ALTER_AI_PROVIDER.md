---
displayed_sidebar: docs
description: "既存の AI プロバイダーのプロパティを更新します。"
---

# ALTER AI PROVIDER

既存の AI プロバイダーの `PROPERTIES` を更新します。指定したキーはプロバイダーの現在のプロパティにマージされます
(既存のキーは上書きされ、指定されなかったキーは保持されます)。プロバイダーの `TYPE` は変更できません。

## 構文

```SQL
ALTER AI PROVIDER [IF EXISTS] <provider_name>
SET ("key" = "value" [, ...])
```

## パラメータ

| パラメータ      | 説明                                                                                            |
| --------------- | ----------------------------------------------------------------------------------------------- |
| `IF EXISTS`     | プロバイダーが存在しない場合、エラーにせず何もしません。                                          |
| `provider_name` | 変更するプロバイダーの名前。                                                                     |
| `SET (...)`     | マージする `"key" = "value"` のペア。許可されるキーはプロバイダーの `TYPE` と同じです ([`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md) を参照)。 |

## 例

プロバイダーの API キーをローテーションし、タイムアウトを延長します。

```sql
ALTER AI PROVIDER openai SET (
    "api_key"    = "sk-new-...",
    "timeout_ms" = "20000"
);
```

## 関連する SQL ステートメント

- [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)
- [`SHOW AI PROVIDERS`](./SHOW_AI_PROVIDERS.md)
- [`DESC AI PROVIDER`](./DESC_AI_PROVIDER.md)
