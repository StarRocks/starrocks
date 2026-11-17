---
displayed_sidebar: docs
description: "AI Provider を使用してクエリに対する候補ドキュメントの関連度を再評価します。"
sidebar_position: 23
toc_max_heading_level: 2
---

# ai_rerank

クエリに対する候補テキストの関連度を評価して並べ替えます。クエリと候補配列は行ごとに異なっていても構いません。`rerank` AI Provider と既存の非同期 AI 実行フレームワークを使用します。

## 構文

```sql
ai_rerank(query, documents[, provider[, model[, options]]])
```

1 回の呼び出しでは位置引数または名前付き引数 (`引数名 => 値`) を使用し、両者を混在させないでください。

## 引数

| 引数 | 型 | 説明 |
| --- | --- | --- |
| `query` | VARCHAR | クエリのテキスト。定数または列の式を指定できます。 |
| `documents` | `ARRAY<VARCHAR>` | 元の順序の候補テキスト。定数または列の式を指定できます。 |
| `provider` | 定数 VARCHAR | 省略、空文字列、SQL NULL はデフォルトの `rerank` Provider を使用します。 |
| `model` | 定数 VARCHAR | 省略、空文字列、SQL NULL は Provider のデフォルトモデルを使用します。 |
| `options` | 定数 JSON | オプションの JSON オブジェクト。省略、`{}`、SQL NULL は追加オプションなしを意味します。 |

`options` の `top_n` は 2147483647 以下の厳密な正の整数で、デフォルトは候補数です。全候補を送信し、`min(top_n, candidate_count)` 件を返します。`top_n` は入力サイズを減らしません。他のオプションはサービスに転送します。`model`、`query`、`documents`、`input`、`parameters` の上書きはできません。

## Provider

`cohere` (`rerank` のデフォルト) と `dashscope_rerank` (DashScope ネイティブテキストリランキング) をサポートします。完全なサービス URL を設定してください。StarRocks はパスを追加しません。DashScope では `https://dashscope.aliyuncs.com/api/v1/services/rerank/text-rerank` と、そのサービスがサポートするモデルを使用します。

候補数が Provider の `max_documents` を超える行は失敗し、切り捨ては行いません。`deadline_ms` はリトライを含む行の要求全体を制限し、クエリと AI ランタイムの期限によっても制限されます。`timeout_ms` は各試行を制限します。既存の AI リトライとエラーポリシーを使用し、元の候補順序へのフォールバックは行いません。

AI 関数と Provider の両方の使用権限が必要です。[CREATE AI PROVIDER](../../sql-statements/cluster-management/ai_provider/CREATE_AI_PROVIDER.md) を参照してください。使用前に FE と BE の両方を対応バージョンへアップグレードしてください。

`max_documents` を省略すると Provider 固有の候補数上限は追加されません。`deadline_ms` を省略しても、既存の AI ランタイムとクエリの時間制限は適用されます。

## 戻り値

NULL 許容の `ARRAY<STRUCT<index INT, relevance_score DOUBLE>>` を、関連度スコアの降順 (同値を許容) で返します。`index` は元の `documents` の **0 始まり**の位置で、SQL 配列添字は **1 始まり**です。同点の場合はサービスの順序を保持します。スコアはサービス固有で、確率ではありません。

- `query` または `documents` が SQL NULL の場合、HTTP 要求なしで SQL NULL を返します。
- クエリが NULL でなく候補配列が空の場合、HTTP 要求なしで `[]` を返します。
- 空文字列と重複した文書は保持します。NULL の文書要素は行レベルのエラーです。
- 件数不一致、重複または範囲外のインデックス、有限でないスコア、昇順のスコアは行レベルのエラーです。応答の修復や並べ替えは行いません。
- `ai_function_on_error = ignore` は失敗した行を NULL にし、`fail` はクエリを中止します。無効な設定やオプション、キャンセル、ローカルリソースの失敗は常に致命的です。

## 例

```sql
CREATE AI PROVIDER reranking TYPE rerank
PROPERTIES (
    "protocol" = "cohere",
    "endpoint" = "https://api.cohere.com/v2/rerank",
    "model" = "YOUR_RERANK_MODEL",
    "api_key" = "YOUR_API_KEY",
    "max_documents" = "1000",
    "deadline_ms" = "10000"
);
SET reranking AS DEFAULT AI PROVIDER;

SELECT ai_rerank(
    query => 'database for real-time analytics',
    documents => ['StarRocks is an analytical database.', 'A recipe for bread.'],
    provider => 'reranking',
    options => json_object('top_n', 1)
);
```

構造体の `index` フィールドは引用符で囲みます。AI 関数を計算してから結果を `UNNEST` に渡してください。

```sql
WITH ranked AS (
    SELECT ai_rerank('database', ['StarRocks', 'bread']) AS hits
)
SELECT hits[1].`index` AS document_index, hits[1].relevance_score AS score
FROM ranked;

WITH ranked AS (
    SELECT ai_rerank('database', ['StarRocks', 'bread']) AS hits
)
SELECT hit.`index`, hit.relevance_score
FROM ranked, UNNEST(hits) AS t(hit);
```

## レート制限と使用量

`ai_function_rate_limit_qps_rerank` のデフォルトは BE のバケットごとに毎秒 4 要求です。バケットはエンドポイント、認証情報、能力で区別されます。リトライも含め、動的更新が可能で、既存のプロセス全体の同時実行上限を共有します。[BE AI 設定](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_rerank) を参照してください。

サービスが明示したトークン数のみ記録します。欠けた値は不明のままで、Cohere の search units をトークンへ変換しません。トークンの推定値や合成した合計値は追加しません。
