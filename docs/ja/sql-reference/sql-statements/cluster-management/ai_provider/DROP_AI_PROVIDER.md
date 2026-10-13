---
displayed_sidebar: docs
description: "AI プロバイダーをクラスターから削除します。"
---

# DROP AI PROVIDER

AI プロバイダーをクラスターから削除します。

現在そのタイプのデフォルトになっているプロバイダーは削除できません。先に別のプロバイダーをそのタイプの
デフォルトに設定してください ([`SET DEFAULT AI PROVIDER`](./SET_DEFAULT_AI_PROVIDER.md) を参照)。

## 構文

```SQL
DROP AI PROVIDER [IF EXISTS] <provider_name>
```

## パラメータ

| パラメータ      | 説明                                                               |
| --------------- | ----------------------------------------------------------------- |
| `IF EXISTS`     | プロバイダーが存在しない場合、エラーにせず何もしません。             |
| `provider_name` | 削除するプロバイダーの名前。                                        |

## 例

```sql
DROP AI PROVIDER openai;
DROP AI PROVIDER IF EXISTS cohere_rerank;
```

## 関連する SQL ステートメント

- [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)
- [`SET DEFAULT AI PROVIDER`](./SET_DEFAULT_AI_PROVIDER.md)
