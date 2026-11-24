---
displayed_sidebar: docs
description: "chat または embedding タイプの外部サービスをクラスターの AI Provider として登録します。"
products: [oss, enterprise, cloud, anywhere]
---

# CREATE AI PROVIDER

外部 AI サービスプロバイダーをクラスターに登録します。単一の統合された AI プロバイダーレジストリが、
異なる**タイプ**のプロバイダーを保持し、**タイプごとに**1 つのデフォルトを保持します。

- `embedding` — [ai_embed](../../../sql-functions/ai-functions/ai_embed.md) に埋め込みエンドポイントを提供します。セマンティックコンテキストモジュールでは `CONTEXT UPSERT` による書き込みと `query_text` による検索に OpenAI 互換の `/v1/embeddings` エンドポイントを使用します。
- `chat` — [ai_complete](../../../sql-functions/ai-functions/ai_complete.md) などの関数にチャット生成エンドポイントを提供します。

プロバイダーオブジェクトは `api_key` プロパティを含めて FE のメタデータジャーナルとイメージに永続化されるため、
クラスターの再起動やアップグレード後も認証情報は保持されます。これらの設定に対応する `fe.conf` の項目はありません。

> **注意**
>
> FE のメタイメージ / BDB ジャーナルディレクトリへの読み取りアクセス権を持つ人は誰でもプロバイダーの `api_key`
> の値を読むことができます。これらのファイルはファイルシステムの権限で保護してください。

SQL の AI 関数で使用する `openai` および `dashscope` Provider は、API キーの有無にかかわらず HTTP と HTTPS をサポートします。空でない key は `Authorization: Bearer ...` ヘッダーになります。URL 形式、制御文字、DNS、アドレスの検証は引き続き適用されます。信頼できるエンドポイントと送信が許可されたデータを使用してください。

`SET <provider_name> AS DEFAULT AI PROVIDER` でデフォルトを設定します。該当するデフォルトが未設定の場合、暗黙 Provider の呼び出しは `EXPLAIN` を含め FE でエラーになります。各実行プランは選択した Provider の設定を固定します。ALTER とデフォルト変更は新たに計画するクエリに反映され、プリペアド文の各 `EXECUTE` も現在のメタデータで再計画します。

## 権限

プロバイダーの作成や管理には SYSTEM `OPERATE` が必要です。AI 関数から呼び出す場合は、[AI 関数の権限](../../../sql-functions/ai-functions/ai_functions.mdx#ai-function-privileges)と、その AI PROVIDER の `USAGE` の両方が必要です。作成しても一般ユーザーに使用権限は自動付与されません。

## 構文

```SQL
CREATE AI PROVIDER [IF NOT EXISTS] <provider_name>
TYPE { embedding | chat }
[ COMMENT '<comment>' ]
PROPERTIES (
    "endpoint"   = "<url>",
    "model"      = "<model_name>"
    [, "protocol" = "{ openai | dashscope }" ]
    [, "dimensions" = "<int>" ]      -- embedding only
    [, "timeout_ms" = "<int>" ]
    [, "api_key" = "<key>" ]
)
```

## パラメータ

| パラメータ      | 説明                                                                                                 |
| --------------- | ---------------------------------------------------------------------------------------------------- |
| `provider_name` | プロバイダーの名前。`SET ... AS DEFAULT AI PROVIDER` で使用され、`SHOW AI PROVIDERS` に表示されます。 |
| `TYPE`          | `embedding` または `chat`。許可されるプロパティと、このプロバイダーがどのタイプのデフォルトになれるかを決定します。 |
| `COMMENT`       | オプションのコメント文字列。                                                                         |
| `PROPERTIES`    | `"key" = "value"` 形式の設定。許可されるキーは `TYPE` によって異なります (以下を参照)。それ以外のキーは拒否されます。 |

### PROPERTIES

| プロパティ      | タイプ         | 必須     | 説明                                                                                                 |
| --------------- | -------------- | -------- | ---------------------------------------------------------------------------------------------------- |
| `endpoint`      | すべて         | はい     | HTTP(S) エンドポイントの URL。`http://` または `https://` で始まる必要があります。                    |
| `model`         | すべて         | はい     | リクエストの `model` フィールドに渡されるモデル名 (例: `text-embedding-3-small`)。 |
| `protocol`      | すべて         | いいえ   | SQL の AI 関数は、OpenAI 互換プロトコル (`openai`) と Alibaba Cloud Model Studio のネイティブプロトコル (`dashscope`) に対応しています。`chat` と `embedding` のデフォルトは `openai` です。 |
| `dimensions`    | embedding      | いいえ   | 埋め込みベクトルの次元数 (正の整数)。プロバイダーの出力およびベクトルインデックスの次元と一致する必要があります。 |
| `timeout_ms`    | すべて         | いいえ   | HTTP リクエストごとのタイムアウト（ミリ秒単位の正の整数）。 |
| `api_key`       | すべて         | いいえ   | `Authorization` ヘッダーに使用する Bearer トークン。認証が不要なローカルプロバイダーの場合は省略します。 |

### SQL の AI 関数で使用するエンドポイント

Alibaba Cloud Model Studio（Bailian）の `/compatible-mode/v1/...` エンドポイントには `openai` を、ネイティブエンドポイントには `dashscope` を指定します。

`dashscope` の場合は、使用する操作のネイティブ API の完全な URL を `endpoint` に指定します。パスと Provider の `TYPE` は一致する必要があります。SQL 呼び出しで `model` を上書きしても、リクエストは設定済みのエンドポイントに送られます。

| Provider タイプ | 用途 | ネイティブエンドポイントのパス |
| --- | --- | --- |
| `chat` | テキスト生成 | `/api/v1/services/aigc/text-generation/generation` |
| `chat` | マルチモーダル生成 | `/api/v1/services/aigc/multimodal-generation/generation` |
| `embedding` | テキスト埋め込み | `/api/v1/services/embeddings/text-embedding/text-embedding` |
| `embedding` | マルチモーダル埋め込み | `/api/v1/services/embeddings/multimodal-embedding/multimodal-embedding` |

各エンドポイントが受け付ける入力は [FILE 入力の対応表](../../../sql-functions/ai-functions/ai_functions.mdx#file-input)を参照してください。モデルが対応する形式は [Model Studio のマルチモーダル埋め込み API](https://www.alibabacloud.com/help/en/model-studio/multimodal-embedding-api-reference) を参照してください。

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

次の例では、Model Studio のネイティブなマルチモーダル生成エンドポイントを使う `chat` Provider を登録します。

```sql
CREATE AI PROVIDER dashscope_chat TYPE chat
PROPERTIES (
    "protocol" = "dashscope",
    "endpoint" = "https://dashscope.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation",
    "model"    = "qwen3-vl-plus",
    "api_key"  = "YOUR_DASHSCOPE_API_KEY"
);
```

## 関連する SQL ステートメント

- [`ALTER AI PROVIDER`](./ALTER_AI_PROVIDER.md) — 既存プロバイダーのプロパティを変更します。
- [`DROP AI PROVIDER`](./DROP_AI_PROVIDER.md) — プロバイダーを削除します。
- [`SHOW AI PROVIDERS`](./SHOW_AI_PROVIDERS.md) — プロバイダーを一覧表示します (`TYPE` でフィルタリング可能)。
- [`DESC AI PROVIDER`](./DESC_AI_PROVIDER.md) — 1 つのプロバイダーの完全な設定を表示します。
- [`SET DEFAULT AI PROVIDER`](./SET_DEFAULT_AI_PROVIDER.md) — プロバイダーをそのタイプのデフォルトに設定します。
