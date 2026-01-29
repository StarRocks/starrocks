---
displayed_sidebar: docs
---

# Query detail API

**query detail** API は FE メモリにキャッシュされたクエリ実行詳細を返します。FE 設定
`enable_collect_query_detail_info` が有効な場合のみ記録されます。

## エンドポイント

- `GET /api/query_detail` (v1)
- `GET /api/v2/query_detail` (v2、`is_request_all_frontend` をサポート、ラップ結果)

## パラメータ

| パラメータ | 必須 | デフォルト | 説明 |
| --- | --- | --- | --- |
| `event_time` | はい | - | 下限フィルタ。`eventTime` がこの値より大きいレコードを返します。`0` を指定すると全件取得できます。 |
| `user` | いいえ | - | `user` フィールドでフィルタ（大文字小文字は区別しません）。 |
| `is_request_all_frontend` | いいえ（v2 のみ） | `false` | `true` の場合、他の存活 FE からも取得してマージします。 |

## レスポンス

- v1 は QueryDetail 配列を返します。
- v2 は `{ "code": "0", "message": "OK", "result": [ ... ] }` を返し、`result` が QueryDetail 配列です。

## 認証と権限

この API は HTTP Basic 認証が必要です。ログイン認証以外の追加権限チェックはありません。認証済みユーザーであれば
誰でもアクセスでき、`user` パラメータでフィルタしない限りキャッシュされた query detail をすべて参照できます。

### QueryDetail フィールド

| フィールド | 型 | 説明 |
| --- | --- | --- |
| `queryId` | string | クエリ ID。 |
| `eventTime` | long | フィルタ用の内部タイムスタンプ（壁時計由来の単調ナノ秒）。 |
| `isQuery` | boolean | クエリかどうか。 |
| `remoteIP` | string | クライアント IP、または `System`。 |
| `connId` | int | 接続 ID。 |
| `startTime` | long | 開始時刻（ミリ秒）。 |
| `endTime` | long | 終了時刻（ミリ秒、未完了は `-1`）。 |
| `latency` | long | 実行時間（ミリ秒、未完了は `-1`）。 |
| `pendingTime` | long | 待機時間（ミリ秒）。 |
| `netTime` | long | 純実行時間（ミリ秒）。 |
| `netComputeTime` | long | 純計算時間（ミリ秒）。 |
| `state` | string | `RUNNING`/`FINISHED`/`FAILED`/`CANCELLED`。 |
| `database` | string | 現在の DB。 |
| `sql` | string | SQL 文（必要に応じてマスク）。 |
| `user` | string | ログインユーザー（qualified user）。 |
| `impersonatedUser` | string | `EXECUTE AS` の対象ユーザー。未使用時は `null`。 |
| `errorMessage` | string | 失敗時のエラー。 |
| `explain` | string | 実行計画（`query_detail_explain_level` に依存）。 |
| `profile` | string | Profile（存在する場合）。 |
| `resourceGroupName` | string | リソースグループ名。 |
| `scanRows` | long | スキャン行数。 |
| `scanBytes` | long | スキャンバイト数。 |
| `returnRows` | long | 返却行数。 |
| `cpuCostNs` | long | CPU コスト（ナノ秒）。 |
| `memCostBytes` | long | メモリコスト（バイト）。 |
| `spillBytes` | long | スピル量（バイト）。 |
| `cacheMissRatio` | float | キャッシュミス率（0-100%）。 |
| `warehouse` | string | Warehouse 名。 |
| `digest` | string | SQL ダイジェスト。 |
| `catalog` | string | Catalog 名。 |
| `command` | string | MySQL コマンド名。 |
| `preparedStmtId` | string | プリペアドステートメント ID。 |
| `queryFeMemory` | long | FE 側メモリ使用量（バイト）。 |
| `querySource` | string | クエリ種別：`EXTERNAL`/`INTERNAL`/`MV`/`TASK`。 |

## 例

### v1

```bash
curl -u root: "http://fe_host:fe_http_port/api/query_detail?event_time=0"
```

### v2

```bash
curl -u root: "http://fe_host:fe_http_port/api/v2/query_detail?event_time=0&is_request_all_frontend=true"
```
