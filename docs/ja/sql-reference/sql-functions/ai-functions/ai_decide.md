---
displayed_sidebar: docs
description: "SYSTEM 決定モデルで業務状態を評価し、検証済み JSON 回答を返します。"
sidebar_position: 70
---

# ai_decide

管理者が設定した Alibaba Cloud Model Studio 決定モデルを呼び出し、検証済みの完全な JSON 応答を返します。質問には分類 (`choice`)、肯定の確率 (`noul`)、順序付き評価 (`score`) を指定できます。[System One 決定 API](https://help.aliyun.com/zh/model-studio/decision-model-api) を使用します。

## 構文

```sql
ai_decide(state VARCHAR, questions JSON) -> JSON
ai_decide(state JSON, questions JSON) -> JSON
ai_decide(model VARCHAR, state VARCHAR, questions JSON) -> JSON
ai_decide(model VARCHAR, state JSON, questions JSON) -> JSON
```

- `state`: VARCHAR/CHAR または JSON 式。テキストは有効な UTF-8 が必要で、JSON や URL に見えてもテキストとして送信されます。JSON は文字列、オブジェクト、配列のみを受け付けます。他の型は明示的な変換または `parse_json` を使用してください。
- `questions`: 1–16 個の質問を定義する定数 JSON オブジェクト。JSON 列や `parse_json(column)` は使用できません。文字列からの暗黙変換は行いません。
- `model`: オプションの定数 VARCHAR 式。省略時は `ai_default_decision_model` を使用します。エンドポイント、プロトコル、認証情報は変更しません。明示的な SQL NULL はデフォルトモデルにフォールバックしません。

options MAP または AI PROVIDER のオーバーロードはありません。既存の [AI 関数権限](ai_functions.mdx#ai-function-privileges)、SQL 配置制約、非同期 AIProject、リクエストのライフサイクルを使用します。使用前にすべての FE と BE をアップグレードしてください。

## 設定

各 FE に独立した決定モデル設定を指定します。

```properties
ai_default_decision_endpoint = https://YOUR_WORKSPACE_ID.cn-beijing.maas.aliyuncs.com/compatible-mode/v1/systemone
ai_default_decision_provider = bailian_decision
ai_default_decision_model = decision-model-preview
```

3 項目のデフォルトは空です。エンドポイントと provider は必須です。すべての呼び出しでモデルを指定する場合、デフォルトモデルは空にできます。[FE 設定](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_decision_endpoint)を参照してください。変更は新しい計画に適用され、既存の計画はスナップショットを保持します。

各 BE の環境変数 `AI_FUNCTION_DECISION_ENDPOINT` は FE の URL と完全一致させ、`AI_FUNCTION_DECISION_API_KEY` に対応する認証情報を設定します。環境変数の変更後は BE を再起動してください。SYSTEM API key は FE の計画に含まれません。chat や embedding の設定にはフォールバックしません。承認された HTTPS エンドポイントに状態と質問が送信されます。

既存のロールへの権限付与例:

```sql
GRANT USAGE ON AI FUNCTION ai_decide TO ROLE decision_reader;
```

テーブル権限も必要です。モデルの利用権限とリージョンの可用性はプロバイダーアカウントに依存します。[モデルドキュメント](https://help.aliyun.com/zh/model-studio/decision-model-preview)を参照してください。

## 質問と結果

質問 ID は空でなく一意である必要があります。定義は `type`、任意の文字列 `instructions`、以下の `criteria` のみを受け付けます。

| 型 | criteria | 回答 |
| --- | --- | --- |
| `choice` | 1–255 個の一意で空でない選択肢名と文字列説明を持つ必須オブジェクト。 | `choice`、全選択肢の正規化された `probabilities`、`[0,1]` の `confidence`。 |
| `noul` | `true` と/または `false` の文字列説明を持つ任意のオブジェクト。 | `[0,1]` の肯定確率。BOOLEAN ではなく、しきい値は呼び出し側が決めます。 |
| `score` | 昇順の 2–255 個の文字列説明を持つ必須配列。 | `[0,N-1]` のゼロ起点の期待レベル。小数も可能です。`legend`、正規化された `probabilities`、`confidence` を含みます。 |

応答には要求した質問 ID が正確に含まれ、型と範囲が有効である必要があります。検証後はオプションのメタデータを含む完全な応答を保持します。SQL JSON 変換で入力の重複キーが正規化される場合がありますが、生のサービス応答の重複キーは拒否されます。

```sql
SELECT ai_decide(
    'The customer reports a duplicate charge and requests a refund.',
    parse_json('{
      "category":{"type":"choice","criteria":{"refund":"Refund request","other":"Other request"}},
      "refund":{"type":"noul","instructions":"Is a refund requested?"},
      "urgency":{"type":"score","criteria":["Routine","Soon","Immediate"]}
    }')
);
```

`result -> '$.answers.refund.noul'` などの JSON 式で結果を抽出できます。評価対象の非 NULL 行ごとに全質問を含む論理リクエストを送信し、再試行で HTTP 回数が増える場合があります。この関数は非決定的です。

## NULL、エラー、使用量

state、questions、明示的 model の SQL NULL は、リクエストなしで SQL NULL を返します。AIProject が実際に初期化される場合、不正な定数質問は、後続の入力行がない場合や state/model が SQL NULL の場合でも fragment 初期化時にエラーになります。`WHERE 1=0` や `LIMIT 0` などでオプティマイザーが除去した呼び出しは初期化も検証もされません。ネイティブ JSON null は SQL NULL と異なり、状態と質問には使用できません。

行入力エラー、HTTP 失敗、不正な応答は [ai_function_on_error](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error) に従います。`ignore` は失敗行に SQL NULL を返し、`fail` はクエリを中止します。設定エラーとクエリのライフサイクル失敗は既存の AI 実行エラーを維持します。エラーを否定回答やゼロ点には変換しません。

有効な `usage.input_tokens` を prompt と total token に計上し、completion token はゼロになります。回答検証が失敗しても信頼できる使用量を保持します。未報告は不明のままとし、不正、重複、負数、オーバーフローは計上しません。token 推定は追加せず、既存のリクエスト、再試行、失敗、実使用量の Profile カウンターを使用します。

[ai_function_rate_limit_qps_decision](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_decision) のデフォルトは、BE、エンドポイント、認証情報、決定能力ごとに毎秒 20 HTTP 試行です。再試行も含まれます。正の値が必要で、動的変更が可能です。グローバル AI 同時実行上限も適用され、クラスター全体のサービスクォータではありません。
