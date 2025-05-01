---
displayed_sidebar: docs
---

# SHOW PROFILELIST

## 説明

StarRocks クラスターにキャッシュされたクエリプロファイルレコードを一覧表示します。クエリプロファイルの詳細については、 [Query Profile Overview](../../../../administration/query_profile_overview.md) を参照してください。

この機能は v3.1 以降でサポートされています。

この操作を行うための特別な権限は必要ありません。

## 構文

```SQL
SHOW PROFILELIST [LIMIT n]
```

## パラメータ

`LIMIT n`: 最新の n 件のレコードを一覧表示します。

## 戻り値

| **戻り値** | **説明**                                                     |
| ---------- | ------------------------------------------------------------ |
| QueryId    | クエリの ID。                                                |
| StartTime  | クエリの開始時間。                                           |
| Time       | クエリのレイテンシー。                                       |
| State      | クエリのステータス。含まれるもの：`Error`: クエリでエラーが発生した。`Finished`: クエリが完了した。`Running`: クエリが実行中。 |
| Statement  | クエリのステートメント。                                     |

## 例

例 1: 最新の 5 件のクエリプロファイルレコードを表示します。

```SQL
SHOW PROFILELIST LIMIT 5;
```

## 関連 SQL

- [ANALYZE PROFILE](./ANALYZE_PROFILE.md)
- [EXPLAIN ANALYZE](./EXPLAIN_ANALYZE.md)