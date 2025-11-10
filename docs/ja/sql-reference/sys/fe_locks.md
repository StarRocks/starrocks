---
displayed_sidebar: docs
---

# fe_locks

`fe_locks` は StarRocks FE のメタデータロックに関する情報を提供します。

`fe_locks` には以下のフィールドが提供されています:

| **フィールド** | **説明**                                                     |
| -------------- | ------------------------------------------------------------ |
| lock_type      | ロックのタイプ（例：「DATABASE」）。                         |
| lock_object    | ロックされているオブジェクト識別子（データベース名、テーブル名など）。 |
| lock_mode      | ロックモード。有効な値：`EXCLUSIVE`、`SHARED`。             |
| start_time     | ロックが取得された時刻。                                     |
| hold_time_ms   | ロックが保持されている時間（ミリ秒）。                       |
| thread_info    | スレッド情報を含む JSON 文字列（threadId, threadName）。    |
| granted        | ロックが現在付与されているかどうか。                         |
| waiter_list    | このロックを待機しているスレッドのリスト（カンマ区切り）。   |

## ロックモード

- **EXCLUSIVE（排他ロック）**：一度に1つのスレッドのみがこのロックを保持できます。
- **SHARED（共有ロック）**：複数のスレッドが同時にこのロックを保持できます。

## ロックタイプ

- **DATABASE**：データベースレベルロック（`lock_manager_enabled` が false の場合）。
- **TABLE**：テーブルレベルロック（`lock_manager_enabled` が true の場合）。

## 設定

`fe_locks` の動作は `lock_manager_enabled` 設定パラメータに依存します：

- `lock_manager_enabled = true` の場合：テーブルレベル粒度で集中ロック管理を行う新しいロックマネージャーを使用。
- `lock_manager_enabled = false` の場合：従来のデータベースレベルロックを使用。

## 例

### 長時間実行ロックの検索

```sql
SELECT lock_object, lock_mode, hold_time_ms, thread_info
FROM information_schema.fe_locks 
WHERE hold_time_ms > 10000  -- 10秒以上保持されているロック
ORDER BY hold_time_ms DESC;
```

### ロック競合の確認

```sql
SELECT lock_object, COUNT(*) as lock_count
FROM information_schema.fe_locks 
WHERE granted = true
GROUP BY lock_object
HAVING COUNT(*) > 1;
```

### 待機スレッドの検索

```sql
SELECT lock_object, waiter_list
FROM information_schema.fe_locks 
WHERE waiter_list != '';
```

## 注意事項

- `fe_locks` のクエリには `OPERATE` 権限が必要です。
- ビューはリアルタイム情報を提供しますが、高いロック活動があるシステムではパフォーマンスに影響する可能性があります。
- 大量のロックを扱う場合は、適切な WHERE 句を使用して結果をフィルタリングしてください。
