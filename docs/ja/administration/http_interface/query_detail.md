---
displayed_sidebar: docs
---

# Query detail API

**Query detail** APIは、FE メモリにキャッシュされている最近の実行されたクエリの詳細を返します。

:::note
Query detail レコードは、FE 構成の `enable_collect_query_detail_info` が `true` に設定されている場合にのみ収集されます。
:::

## エンドポイント

- `GET /api/query_detail`（v1）
- `GET /api/v2/query_detail`（v2、`is_request_all_frontend` をサポートし、ラップされた結果を返します）

## パラメータ

| 名前                      | 必須             | デフォルト | 説明                                                         |
| ------------------------- | ---------------- | ---------- | ------------------------------------------------------------ |
| `event_time`              | はい             | -          | 下限フィルタ条件。`eventTime` がこの値より大きいレコードを返します。`0` を指定するとすべてのキャッシュ済みレコードを返します。 |
| `user`                    | いいえ           | -          | `user` フィールドでフィルタします（大文字・小文字を区別しません）。 |
| `is_request_all_frontend` | いいえ（v2のみ） | `false`    | `true` の場合、現在の FE が他の稼働中 FE に問い合わせ、結果をマージします。 |

## レスポンス

- v1 は QueryDetail 配列を返します。
- v2 は `{ "code": "0", "message": "OK", "result": [ ... ] }` を返し、`result` が QueryDetail 配列です。

## 認証と権限

本 API は **HTTP Basic 認証** を必要とします。

ログインに成功すれば追加の権限制御は行われません。認証済みユーザーは、`user` フィルタを指定しない限り、すべてのキャッシュ済みクエリ詳細を参照できます。

## QueryDetail フィールド

| フィールド名          | 型      | 説明                                                         |
| ------------------- | ------- | ------------------------------------------------------------ |
| `queryId`           | string  | クエリ ID。                                                  |
| `eventTime`         | long    | フィルタに使用される内部タイムスタンプ（ウォールクロック由来の単調増加ナノ秒）。 |
| `isQuery`           | boolean | クエリ文かどうか。                                           |
| `remoteIP`          | string  | クライアント IP アドレス、または `System`。                  |
| `connId`            | int     | コネクション ID。                                            |
| `startTime`         | long    | クエリ開始時刻（エポックからのミリ秒）。                     |
| `endTime`           | long    | クエリ終了時刻（エポックからのミリ秒）。未完了の場合は `-1`。 |
| `latency`           | long    | クエリレイテンシ（ミリ秒）。未完了の場合は `-1`。            |
| `pendingTime`       | long    | Pending 状態の時間（ミリ秒）。                               |
| `netTime`           | long    | 正味実行時間（ミリ秒）。                                     |
| `netComputeTime`    | long    | 正味計算時間（ミリ秒）。                                     |
| `state`             | string  | クエリ状態：`RUNNING`、`FINISHED`、`FAILED`、`CANCELLED`。   |
| `database`          | string  | 現在のデータベース。                                         |
| `sql`               | string  | SQL 文（設定によりマスキングされる場合があります）。         |
| `user`              | string  | ログインユーザー（完全修飾ユーザー名）。                     |
| `impersonatedUser`  | string  | `EXECUTE AS` の対象ユーザー。未使用時は `null`。             |
| `errorMessage`      | string  | クエリ失敗時のエラーメッセージ。                             |
| `explain`           | string  | Explain プラン（`query_detail_explain_level` により制御）。  |
| `profile`           | string  | 収集されている場合の Profile 情報。                          |
| `resourceGroupName` | string  | リソースグループ名。                                         |
| `scanRows`          | long    | スキャンした行数。                                           |
| `scanBytes`         | long    | スキャンしたデータ量（バイト）。                             |
| `returnRows`        | long    | 返却された行数。                                             |
| `cpuCostNs`         | long    | CPU 使用時間（ナノ秒）。                                     |
| `memCostBytes`      | long    | メモリ使用量（バイト）。                                     |
| `spillBytes`        | long    | スピルされたデータ量（バイト）。                             |
| `cacheMissRatio`    | float   | キャッシュミス率（％、0–100）。                              |
| `warehouse`         | string  | Warehouse 名。                                               |
| `digest`            | string  | SQL ダイジェスト。                                           |
| `catalog`           | string  | Catalog 名。                                                 |
| `command`           | string  | MySQL コマンド名。                                           |
| `preparedStmtId`    | string  | プリペアドステートメント ID。                                |
| `queryFeMemory`     | long    | クエリが FE 上で使用したメモリ量（バイト）。                 |
| `querySource`       | string  | クエリの発生元：`EXTERNAL`、`INTERNAL`、`MV`、`TASK`。       |

## 例

### v1

```bash
curl -u root: "http://<fe_host>:<fe_http_port>/api/query_detail?event_time=0"
```

### v2

```bash
curl -u root: "http://<fe_host>:<fe_http_port>/api/v2/query_detail?event_time=0&is_request_all_frontend=true"
```
