---
displayed_sidebar: docs
description: "外部チャットとテキスト埋め込みのためのクラスタ全体の AI モデルを作成、管理、認可します。"
sidebar_position: 23
---

# AI モデル

AI モデルは独立したクラスタ全体のオブジェクトです。Resource ではなく、Database や Catalog にも属しません。大文字と小文字を区別する名前で外部推論サービスを識別します。StarRocks 内で学習したモデルではありません。capability、provider、完全な endpoint、プロバイダーのモデル名、BE ローカル認証情報の参照を保存し、API key は保存しません。

使用前にすべての FE と BE をアップグレードしてください。古いノードは新しいジャーナル操作、権限、実行メタデータを理解できません。混在バージョンでの使用と、このメタデータを作成した後のダウングレードはサポートしません。モデルを削除するだけではジャーナルや権限の履歴は消えません。

:::warning
推論は入力をクラスタ外へ送信し、料金が発生する場合があります。CREATE AI MODEL は運用担当者が用意した認証情報の参照を選べるため、信頼する管理者だけに付与してください。CREATE と EXPLAIN はプロバイダーにアクセスしません。
:::

## 作成と管理

```sql
CREATE AI MODEL [IF NOT EXISTS] model_name
[COMMENT 'description']
PROPERTIES (
    "capability" = "CHAT",
    "provider" = "openai_compatible",
    "endpoint" = "https://models.example.com/v1/chat/completions",
    "model" = "approved-chat-model",
    "credential_ref" = "SUPPORT"
);

ALTER AI MODEL [IF EXISTS] model_name SET ("model" = "approved-chat-model-v2");
ALTER AI MODEL [IF EXISTS] model_name COMMENT = 'Updated description';
SHOW AI MODELS [LIKE 'pattern'];
DESC AI MODEL model_name;
DROP AI MODEL [IF EXISTS] model_name;
```

角括弧は省略可能な構文を示します。model_name は `support_chat` のようなデータベース修飾なしの識別子です。5 個のプロパティはすべて必須です。`type` や生の `api_key` など未知のプロパティは拒否されます。CREATE OR REPLACE と RENAME はサポートしません。

| プロパティ | 要件 |
|------------|------|
| `capability` | `CHAT` または `TEXT_EMBEDDING`。作成後は変更不可。 |
| `provider` | `openai_compatible`。 |
| `endpoint` | ホストを含む完全な HTTPS POST URL。明示ポートは 1–65535。ユーザー情報、クエリ文字列、フラグメント、制御文字は禁止。パスは自動追加しません。 |
| `model` | 空白でなく制御文字を含まないプロバイダーのモデル名。 |
| `credential_ref` | `[A-Z0-9_]{1,64}`。作成後は変更不可。秘密そのものではなく公開参照。 |

ALTER は置換内容全体を検証してから新しい不変リビジョンを公開します。失敗時は元のモデルを維持し、同一値の再設定では新しいリビジョンを作りません。endpoint を変更した場合、新しいクエリを実行する前に BE ローカルのバインドも一致させます。capability や credential_ref の変更には新しいモデルを作成します。

SHOW は呼び出し元に可視の名前だけを返します。DESC は Id、Name、Revision、Capability、Provider、Endpoint、Model、CredentialRef、Comment を返し、秘密は含みません。

テキスト埋め込みには `"capability" = "TEXT_EMBEDDING"` と、例えば `https://models.example.com/v1/embeddings` のような完全なエンドポイントを指定します。

### BE ローカル認証情報

各参照 `<REF>` について、実行するすべての BE に次の環境変数を設定します。

- `AI_FUNCTION_CREDENTIAL_<REF>_ENDPOINT`: モデルの endpoint と完全一致する URL。
- `AI_FUNCTION_CREDENTIAL_<REF>_API_KEY`: 対応する Bearer 認証情報。

SUPPORT は `AI_FUNCTION_CREDENTIAL_SUPPORT_ENDPOINT` と `AI_FUNCTION_CREDENTIAL_SUPPORT_API_KEY` を使用します。この固定命名規則のみを許可します。変更後は対象 BE を再起動します。欠落や不一致は実行エラーになり、SYSTEM 認証情報へフォールバックしません。DNS 検証とアドレス固定は [ai_complete](ai_complete.md#be-ローカル認証情報) と同じです。

## 認可

既存のロール、GRANT/REVOKE、Native 認可、Ranger を使用します。PUBLIC に暗黙の USAGE は与えません。データベース所有者にもモデル権限は自動付与しません。既存ユーザーには以下のように付与できます。

```sql
GRANT CREATE AI MODEL ON SYSTEM TO 'model_admin'@'%';
GRANT USAGE ON AI MODEL support_chat TO 'analyst'@'%';
GRANT ALTER, DROP ON AI MODEL support_chat TO 'model_admin'@'%';
GRANT USAGE ON ALL AI MODELS TO 'ai_service'@'%';
REVOKE USAGE ON AI MODEL support_chat FROM 'analyst'@'%';
```

CREATE は SYSTEM の CREATE AI MODEL、ALTER/DROP はモデルの対応する権限が必要です。SHOW/DESC はモデルに対するいずれかの権限を要求します。呼び出しには USAGE が必要で、ネストしたクエリ、通常ビュー、セキュリティビュー、プリペアドステートメントの各実行にも適用されます。ビューのテーブル権限は呼び出し元のモデル USAGE の代わりにはなりません。

Native のオブジェクト権限は安定したモデル ID に結び付きます。同名の削除と再作成では新しい ID になり、以前のオブジェクト権限を引き継ぎません。ワイルドカード権限は引き続き適用されます。Ranger は `ai_model` の名前ポリシーを使うため、同名の再作成にも適用され得ます。Ranger StarRocks service definition を更新してください。Ranger の拒否から Native 認可へのフォールバックはありません。

## 呼び出し

```sql
ai_custom_query(model_name, text)
ai_custom_query(model_name, text, options)
ai_custom_embedding(model_name, text)
ai_custom_embedding(model_name, text, options)
```

- `model_name`: 正確な名前に解決される NULL でも空白でもない定数 VARCHAR 式。行ごとのプロバイダーモデル名ではありません。
- `text`: VARCHAR 式。URL も文字列として扱います。FILE、画像、マルチモーダル入力は非対応。
- `options`: 省略可能な定数 MAP。型付き NULL MAP は空と見なします。チャットは [ai_complete](ai_complete.md#options-map-のルール)、埋め込みは [ai_embed](ai_embed.md#構文) のルールに従います。
- `ai_custom_query` は CHAT を要求し、NULL 許容 VARCHAR を返します。`ai_custom_embedding` は TEXT_EMBEDDING を要求し、NULL 許容 `ARRAY<FLOAT>` を返します。モデル欠落や capability の不一致は実行前に失敗します。

NULL テキストはリクエストを送りません。既存の[エラーポリシーと SQL 制限](ai_complete.md#制限とセキュリティ)が適用されます。SYSTEM のデフォルト設定は不要です。

文が参照するモデルを認可前にまとめて取得し、認可とすべての AIProject が同じ不変リビジョンを使用します。バインド後の ALTER/DROP は認可済みプランを変更しません。後続の計画では現在のメタデータを取得し、プリペアドステートメントは実行ごとに再計画します。取り消しや削除は実行中クエリを遡及的にキャンセルしません。

Query dump は AI モデルのメタデータを保存しません。名前付きモデル呼び出しのオフライン再生は非対応です。未取得のスナップショットを現在の同名モデルで置き換えることはできません。

```sql
EXPLAIN SELECT ai_custom_query('support_chat', 'A local test prompt.');
EXPLAIN SELECT ai_custom_embedding('search_embedding', 'A local test sentence.');
```

EXPLAIN は HTTP リクエストなしでメタデータと権限を検証します。既存 SYSTEM `ai_complete`、テキスト補助関数、`ai_embed` のルーティングは変更せず、AI モデルオブジェクトを解決しません。
