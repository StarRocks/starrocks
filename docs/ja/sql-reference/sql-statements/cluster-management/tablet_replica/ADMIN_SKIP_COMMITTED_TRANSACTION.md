---
displayed_sidebar: docs
---

# ADMIN SKIP COMMITTED TRANSACTION

共有データ（lake）テーブル上で `COMMITTED` 状態のままスタックしたトランザクションを強制的に解除します。「no-op publish」を 1 回実行することで、当該トランザクションのデータ寄与を破棄しつつ、その分のデータ変更を一切含まない新しい tablet メタデータファイルを書き出してパーティションの可視バージョンを前進させます。

これは運用者向けの **非常用エスケープハッチ** であり、publish キューが `txnlog` の欠落、segment / SST データファイルの消失、オブジェクトストレージ障害などで **永久にスタック** した状態を解除するために用います。当該トランザクションの **データを破棄** するため、通常の再試行で復旧できない場合に限り、drop partition のようなより破壊的な復旧手段より優先して使用してください。

:::warning

`COMMITTED` 済みのトランザクションを skip するのは **不可逆** であり、当該トランザクションのデータが失われます。トランザクションレコード自体は（パーティションのバージョン連続性を保つため）`VISIBLE` として終了しますが、内部的には監査用に no-op publish のマーカーが付与されます。

:::

:::tip

本操作には SYSTEM レベルの OPERATE 権限が必要です。権限付与については [GRANT](../../account-management/GRANT.md) を参照してください。

:::

## 構文

```sql
ADMIN SKIP COMMITTED TRANSACTION <txn_id> [REASON '<text>']
```

## パラメーター

| パラメーター | 必須 | 説明 |
|---|---|---|
| `txn_id` | はい | トランザクションの数値 ID。[SHOW TRANSACTION](../../loading_unloading/SHOW_TRANSACTION.md) または FE ログから取得します。 |
| `REASON '<text>'` | いいえ | skip の理由（自由記述）。FE 監査ログに書き込まれます。事後レビューのため記入を強く推奨します。 |

## 制約（第 1 フェーズ）

1. **FE 設定で有効化が必要**。FE 設定 `enable_admin_skip_committed_txn` が `false` のとき、この文はエラーで拒否されます（デフォルト `false`、誤操作防止）。有効化方法：

   ```sql
   ADMIN SET FRONTEND CONFIG ("enable_admin_skip_committed_txn" = "true");
   ```

   復旧完了後、ただちに無効化してください。

2. **トランザクションは `COMMITTED` 状態でなければならない**。`PREPARED` のトランザクションはタイムアウトで自動的に中止されるか、各ロード種別固有の取消方法（broker / spark / routine load では `CANCEL LOAD`、stream load では `POST /api/transaction/rollback`）を使用してください。すでに `VISIBLE` のトランザクションは取り消し不可能です。

3. **`file_bundling=true` の共有データ（cloud-native）テーブルのみ対応**。`file_bundling` モードではパーティションのメタデータが aggregator により原子的に書き出されるため、no-op publish はパーティション全体に対して効くか全く効かないかのいずれかです。`file_bundling` を有効化していないテーブルは partial-publish 状態に陥るリスクがあり、入口で拒否されます。

4. **本リリースではロード系および lake-compaction 種別のトランザクションのみ対応**。Alter / schema-change 種別のトランザクションは未対応で、試行するとエラーになります。

## 動作

1. FE がトランザクションの状態、ソース種別、関連テーブルが制約を満たすかを検証します。
2. FE が no-op publish マーカーをトランザクションに書き込み、edit log に永続化します。
3. 次回の publish daemon tick で、FE が publish RPC を通じてマーカーを BE に伝播します。
4. BE がトランザクションのターゲットバージョンに、ベースバージョンと同等の内容（当該トランザクションのデータ変更を含まない）の tablet メタデータファイルを書き出します。`file_bundling` 配下では aggregator がパーティション全体の原子性を保証します。
5. パーティションの可視バージョンがトランザクションのターゲットバージョンまで前進し、後続のトランザクションは通常通り publish できます。
6. 当該トランザクションの最終状態は `VISIBLE` として記録されますが、監査のため no-op publish マーカーが残されます。

競合解決：もし元の publish RPC が偶然先に成功した場合、トランザクションは通常通り `VISIBLE` となり、マーカーは no-op となります（データは破棄されず保たれる）。この結果は FE ログに記録されます。

## 監査

`SHOW PROC '/transactions/<db_name>/finished'` には末尾に 2 つのカラムが追加されています：

- `NoOpPublish`：`true`/`false`。当該トランザクションが no-op publish 経路で最終化されたかどうかを示します。
- `NoOpPublishReason`：`ADMIN SKIP COMMITTED TRANSACTION` 文の `REASON` に渡されたテキスト。

事後レビュー時にこれらのカラムで skip が実際に効いたか、当時記入された理由を確認できます。

## 例

1. スタックしたトランザクションを探します：

   ```sql
   SHOW PROC '/transactions/<db_name>/running';
   ```

2. スイッチを有効化します：

   ```sql
   ADMIN SET FRONTEND CONFIG ("enable_admin_skip_committed_txn" = "true");
   ```

3. スタックしたトランザクションを skip します：

   ```sql
   ADMIN SKIP COMMITTED TRANSACTION 12345 REASON 'OSS 上の txnlog が消失、手動復旧';
   ```

4. スイッチをふたたび無効化します：

   ```sql
   ADMIN SET FRONTEND CONFIG ("enable_admin_skip_committed_txn" = "false");
   ```

## 関連コマンド

- [`SHOW TRANSACTION`](../../loading_unloading/SHOW_TRANSACTION.md) — トランザクションの状態を確認します。
- [`ADMIN REPAIR`](./ADMIN_REPAIR.md) — メタデータが復旧不能な場合に lake パーティションを過去の利用可能バージョンへロールバックします。本コマンドが適用できないときのより破壊的な代替手段です。
