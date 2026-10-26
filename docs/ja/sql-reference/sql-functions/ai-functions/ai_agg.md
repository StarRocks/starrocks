---
displayed_sidebar: docs
description: "SYSTEM Chat モデルを使用し、指示に従ってグループのテキストを集約します。"
sidebar_position: 24
---

# ai_agg

`ai_agg` は、指示に従って各 SQL グループのテキストを要約し、グループごとに NULL 許容の VARCHAR を返します。`count` や `sum` などの通常の集約と併用できます。

`ai_agg` は [ai_complete](ai_complete.md#設定) の SYSTEM Chat 設定と BE ローカル認証情報を使用します。使用前にすべての FE と BE をアップグレードしてください。

:::warning
入力テキスト、中間要約、指示は設定済みのモデルサービスへ送信されます。1 グループで複数のリクエストと再試行が発生し、課金される場合があります。他の AI 関数と同じデータ取り扱いとエンドポイント承認の方針を適用してください。
:::

## 構文

```sql
ai_agg(expr, instruction)
ai_agg(expr, instruction, options)
```

- `expr`: VARCHAR 式。NULL はスキップし、空文字列と重複値は保持します。FILE 入力はサポートしません。
- `instruction`: 求める結果を記述する、非 NULL、空白以外の定数 VARCHAR 式。
- `options`: [ai_complete のオプション規則](ai_complete.md#options-map-のルール) に従うオプションの定数 MAP。MAP 型の NULL は空 MAP として扱います。`response_format` は最終リクエストにのみ適用します。1 回のリクエストだけで処理する場合も同様です。その他のオプションは各リクエストに適用します。`max_tokens` などは各リクエストの上限であり、グループ全体のトークン上限ではありません。

`DISTINCT`、集約内の `ORDER BY`、ウィンドウ実行、集約状態コンビネータはサポートしません。正確なフィルタリング、重複排除、件数、計算が必要な場合は、AI 集約の前に SQL で処理してください。入力順序は保証されず、モデル出力は非決定的です。階層的な要約では詳細が失われることがあるため、正確な計算や網羅的な抽出は保証しません。

## 戻り値とエラー

入力が空のグローバル集約、またはすべて NULL のグループは、モデルを呼び出さず NULL を返します。グループ化したクエリに入力行がない場合は、通常の SQL 集約と同じくグループを返しません。空文字列は集約に含まれます。

モデルおよび縮約の予算エラーは既存の [ai_function_on_error](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error) に従います。`ignore` は**グループ全体**に NULL を返し、`fail` はクエリを中止します。成功した一部のリクエストだけの要約は返しません。設定エラー、不正な集約状態、ネイティブ集約のグループサイズ超過、ローカルリソースエラー（スケジューリングキューの枯渇やメモリエラーを含む）、キャンセル、クエリ期限超過は常にクエリを失敗させます。サニタイズ済みのローカルリソースエラーでは元の原因を復元できないため、一般的なリソースエラーを返します。直接検出したメモリ割り当て失敗は、メモリ制限エラーのまま返します。

初期実装には次の固定上限があります。

| 対象 | 上限 |
| --- | --- |
| グループごとのエンコード済み入力 | 1 MiB。24 バイトのヘッダーと非 NULL レコードごとの 8 バイトを含む |
| 各リクエストのプロンプト | 64 KiB。指示、レコードのエスケープ、プロンプトのラッパーを含む |
| 中間リクエストのオプション | キーとシリアライズ済み JSON 値の合計 64 KiB。`response_format` を除き、最終リクエストが 1 回だけの場合は適用しない |
| 各リクエストの中間要約 | 4 KiB |
| グループで保持する中間要約 | 現在と次のラウンドの合計 1 MiB |
| グループごとのラウンド数 | 8 |
| グループごとの論理リクエスト数 | 1024。再試行を除く |

プロンプトに収まらないレコードは切り詰めずに拒否します。要約が大きすぎる場合や十分に縮約できない場合は、グループを失敗させます。バイト上限はトークン推定ではなく、すべての Provider のコンテキストウィンドウに収まる保証もありません。既存のレスポンスサイズ、レート、同時実行、再試行、メモリ、クエリ期限の制御も適用されます。リクエストのタイムアウトは論理リクエストごと、クエリ期限は集約全体に適用されます。

## 権限と例

`USAGE ON AI FUNCTION ai_agg` または `USAGE ON ALL AI FUNCTIONS` が必要です。その他の認可と実行コンテキストについては [AI 関数の権限](ai_functions.mdx#ai-function-privileges) を参照してください。

`reviews(category, review_text)` テーブルと SYSTEM Chat モデルがある場合:

```sql
SELECT category, count(*) AS review_count,
       ai_agg(review_text, 'Summarize the main positive and negative themes.') AS summary
FROM reviews
GROUP BY category;
```

リクエストごとにオプションを指定する場合:

```sql
SELECT category,
       ai_agg(review_text, 'Summarize the recurring issues.',
              map{'temperature': 0.0}) AS summary
FROM reviews
WHERE review_text IS NOT NULL
GROUP BY category;
```

`EXPLAIN` はプランを検証・表示し、モデルを呼び出しません。Query Profile にはグループ数、空グループ数、リーフリクエスト、ラウンド、ローカル縮約予算エラーが表示されます。既存の AI リクエスト、再試行、実測トークンの統計には、失敗前の処理を含む全ラウンドの消費が計上されます。
