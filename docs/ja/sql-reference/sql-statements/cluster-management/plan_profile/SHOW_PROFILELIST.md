---
displayed_sidebar: docs
---

# SHOW PROFILELIST

## 説明

StarRocks クラスターにキャッシュされている query profile レコードを一覧表示します。query profile についての詳細は、[Query Profile Overview](../../../../administration/query_profile_overview.md) を参照してください。

この機能は v3.1 以降でサポートされています。

この操作を行うための特別な権限は必要ありません。

## 構文

```SQL
SHOW PROFILELIST [LIMIT n]
```

## パラメータ

`LIMIT n`: 最新の n 件のレコードを一覧表示します。

## 戻り値

| **戻り値** | **説明**                                                      |
| ---------- | ------------------------------------------------------------- |
| QueryId    | クエリの ID。                                                 |
| StartTime  | クエリの開始時間。                                            |
| Time       | クエリのレイテンシー。                                        |
| State      | クエリの状態。`Error`: クエリでエラーが発生した場合。`Finished`: クエリが完了した場合。`Running`: クエリが実行中の場合。 |
| Statement  | クエリのステートメント。                                      |

## 例

例 1: 最新の 5 件の query profile レコードを表示します。

```SQL
SHOW PROFILELIST LIMIT 5;
```

## 関連 SQL

- [ANALYZE PROFILE](./ANALYZE_PROFILE.md)
- [EXPLAIN ANALYZE](./EXPLAIN_ANALYZE.md)