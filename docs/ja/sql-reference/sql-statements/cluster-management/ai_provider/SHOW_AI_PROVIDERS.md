---
displayed_sidebar: docs
description: "登録済みの AI プロバイダーを一覧表示します。名前のパターンまたはタイプでフィルタリングできます。"
---

# SHOW AI PROVIDERS

登録済みの AI プロバイダーをプロバイダーごとに 1 行で一覧表示します。`api_key` 列は常にマスクされます。
結果は名前のパターンまたはプロバイダーのタイプでフィルタリングできます。

## 構文

```SQL
SHOW AI PROVIDERS [ LIKE '<pattern>' | TYPE { embedding | rerank | text } ]
```

## パラメータ

| パラメータ         | 説明                                                                        |
| ------------------ | --------------------------------------------------------------------------- |
| `LIKE '<pattern>'` | 名前が SQL の `LIKE` パターンに一致するプロバイダーのみを表示します。          |
| `TYPE <type>`      | 指定したタイプ (`embedding`、`rerank`、または `text`) のプロバイダーのみを表示します。 |

## 戻り値の列

| 列             | 説明                                                                 |
| -------------- | -------------------------------------------------------------------- |
| `Name`         | プロバイダーの名前。                                                  |
| `Type`         | プロバイダーのタイプ (`embedding` / `rerank` / `text`)。              |
| `IsDefault`    | このプロバイダーがそのタイプのデフォルトかどうか (`true`/`false`)。    |
| `Endpoint`     | エンドポイントの URL。                                                |
| `Model`        | モデル名。                                                            |
| `Dimensions`   | 埋め込みの次元数 (embedding プロバイダー)。                            |
| `MaxDocuments` | 1 回の rerank リクエストあたりのドキュメントの最大数 (rerank プロバイダー)。 |
| `TimeoutMs`    | リクエストごとの HTTP タイムアウト (ミリ秒)。                          |
| `ApiKey`       | マスクされた API キー。                                               |
| `Comment`      | プロバイダーのコメント。                                              |

## 例

```sql
SHOW AI PROVIDERS;
SHOW AI PROVIDERS LIKE 'open%';
SHOW AI PROVIDERS TYPE rerank;
```

## 関連する SQL ステートメント

- [`CREATE AI PROVIDER`](./CREATE_AI_PROVIDER.md)
- [`DESC AI PROVIDER`](./DESC_AI_PROVIDER.md)
