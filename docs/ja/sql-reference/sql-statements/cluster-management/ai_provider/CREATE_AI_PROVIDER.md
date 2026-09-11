---
displayed_sidebar: docs
description: "外部 AI サービスプロバイダー (embedding または rerank) を SQL で管理されるクラスターメタデータとして登録します。"
---

# CREATE AI PROVIDER

外部 AI サービスプロバイダーをクラスターに登録します。単一の統合された AI プロバイダーレジストリが、
異なる**タイプ**のプロバイダーを保持し、**タイプごとに**1 つのデフォルトを保持します。

- `embedding` — OpenAI 互換の `/v1/embeddings` エンドポイント。セマンティックコンテキストモジュールが
  `CONTEXT UPSERT` のコンテンツや `query_text` 検索を埋め込む際に使用されます。
- `rerank` — Cohere 互換の `/rerank` エンドポイント (Cohere / Jina / Voyage / OpenRouter / ローカル TEI)。
  `/api/context/search` のオプションであるクロスエンコーダーによる第 2 フェーズで使用されます。
- `text` — 将来のテキスト生成 / 推論プロバイダー向けに予約されています。

プロバイダーオブジェクトは `api_key` プロパティを含めて FE のメタデータジャーナルとイメージに永続化されるため、
クラスターの再起動やアップグレード後も認証情報は保持されます。これらの設定に対応する `fe.conf` の項目はありません。

> **注意**
>
> FE のメタイメージ / BDB ジャーナルディレクトリへの読み取りアクセス権を持つ人は誰でもプロバイダーの `api_key`
> の値を読むことができます。これらのファイルはファイルシステムの権限で保護してください。

## 構文

```SQL
CREATE AI PROVIDER [IF NOT EXISTS] <provider_name>
TYPE { embedding | rerank | text }
[ COMMENT '<comment>' ]
PROPERTIES (
    "endpoint"   = "<url>",
    "model"      = "<model_name>"
    [, "dimensions" = "<int>" ]      -- embedding only
    [, "max_documents" = "<int>" ]   -- rerank only (optional)
    [, "deadline_ms" = "<int>" ]     -- rerank only (optional)
    [, "timeout_ms" = "<int>" ]
    [, "api_key" = "<key>" ]
)
```

## パラメータ

| パラメータ      | 説明                                                                                                 |
| --------------- | ---------------------------------------------------------------------------------------------------- |
| `provider_name` | プロバイダーの名前。`SET ... AS DEFAULT AI PROVIDER` で使用され、`SHOW AI PROVIDERS` に表示されます。 |
| `TYPE`          | `embedding`、`rerank`、または `text`。許可されるプロパティと、このプロバイダーがどのタイプのデフォルトになれるかを決定します。 |
| `COMMENT`       | オプションのコメント文字列。                                                                         |
| `PROPERTIES`    | `"key" = "value"` 形式の設定。許可されるキーは `TYPE` によって異なります (以下を参照)。それ以外のキーは拒否されます。 |

### PROPERTIES

| プロパティ      | タイプ         | 必須     | 説明                                                                                                 |
| --------------- | -------------- | -------- | ---------------------------------------------------------------------------------------------------- |
| `endpoint`      | すべて         | はい     | HTTP(S) エンドポイントの URL。`http://` または `https://` で始まる必要があります。                    |
| `model`         | すべて         | はい     | リクエストの `model` フィールドに渡されるモデル名 (例: `text-embedding-3-small`、`cohere/rerank-4-fast`)。 |
| `dimensions`    | embedding      | いいえ   | 埋め込みベクトルの次元数 (正の整数)。プロバイダーの出力およびベクトルインデックスの次元と一致する必要があります。 |
| `max_documents` | rerank         | いいえ   | 1 回の rerank リクエストで送信するドキュメントの最大数 (正の整数、デフォルト 1000)。                  |
| `deadline_ms`   | rerank         | いいえ   | すべてのリトライを含む rerank 呼び出し全体の実時間の上限 (ミリ秒、正の整数、デフォルト 10000)。応答の遅い、または到達不能なリランカーが検索をどれだけ長く停止させられるかを制限し、超過した場合はフュージョン順にフォールバックします。タイムアウトはリトライされません。リトライされるのは接続失敗または HTTP 5xx のみです。 |
| `timeout_ms`    | すべて         | いいえ   | リクエストごとの HTTP タイムアウト (ミリ秒、正の整数)。rerank の場合、実質的に残りの `deadline_ms` によって上限が決まります。 |
| `api_key`       | すべて         | いいえ   | `Authorization` ヘッダーに使用する Bearer トークン。認証が不要なローカルプロバイダーの場合は省略します。 |

## 例

embedding プロバイダーを登録し、`embedding` タイプのデフォルトに設定します。

```sql
CREATE AI PROVIDER openai TYPE embedding
PROPERTIES (
    "endpoint"   = "https://api.openai.com/v1/embeddings",
    "model"      = "text-embedding-3-small",
    "dimensions" = "1536",
    "api_key"    = "sk-..."
);
SET openai AS DEFAULT AI PROVIDER;   -- becomes the default embedding provider
```

rerank プロバイダーを登録し、`rerank` タイプのデフォルトに設定します (embedding のデフォルトとは独立しています)。

```sql
CREATE AI PROVIDER cohere_rerank TYPE rerank
PROPERTIES (
    "endpoint"   = "https://openrouter.ai/api/v1/rerank",
    "model"      = "cohere/rerank-4-fast",
    "timeout_ms" = "15000",
    "api_key"    = "sk-or-..."
);
SET cohere_rerank AS DEFAULT AI PROVIDER;   -- becomes the default rerank provider
```

## 関連する SQL ステートメント

- [`ALTER AI PROVIDER`](./ALTER_AI_PROVIDER.md) — 既存プロバイダーのプロパティを変更します。
- [`DROP AI PROVIDER`](./DROP_AI_PROVIDER.md) — プロバイダーを削除します。
- [`SHOW AI PROVIDERS`](./SHOW_AI_PROVIDERS.md) — プロバイダーを一覧表示します (`TYPE` でフィルタリング可能)。
- [`DESC AI PROVIDER`](./DESC_AI_PROVIDER.md) — 1 つのプロバイダーの完全な設定を表示します。
- [`SET DEFAULT AI PROVIDER`](./SET_DEFAULT_AI_PROVIDER.md) — プロバイダーをそのタイプのデフォルトに設定します。
