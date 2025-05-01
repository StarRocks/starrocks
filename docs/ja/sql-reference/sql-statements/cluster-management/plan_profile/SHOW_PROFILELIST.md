---
displayed_sidebar: docs
---

# SHOW PROFILELIST

## 説明

StarRocks クラスターにキャッシュされたクエリプロファイルレコードを一覧表示します。クエリプロファイルの詳細については、 [Query Profile Overview](../../../../administration/query_profile_overview.md) を参照してください。

この機能は v3.1 以降でサポートされています。

この操作を実行するための特権は必要ありません。

## 構文

```SQL
SHOW PROFILELIST [LIMIT n]
```

## パラメータ

`LIMIT n`: 最新の n 件のレコードを一覧表示します。

## 戻り値

| **戻り値** | **説明**                                                   |
| ---------- | ---------------------------------------------------------- |
| QueryId    | クエリのID。                                               |
| StartTime  | クエリの開始時間。                                         |
| Time       | クエリのレイテンシー。                                     |
| State      | クエリのステータス。含まれるもの：`Error`: クエリにエラーが発生。`Finished`: クエリが完了。`Running`: クエリが実行中。 |
| Statement  | クエリのステートメント。                                   |

## 例

例 1: 最新のクエリプロファイルレコードを5件表示します。

```SQL
SHOW PROFILELIST LIMIT 5;
```

## 関連する SQL

- [ANALYZE PROFILE](./ANALYZE_PROFILE.md)
- [EXPLAIN ANALYZE](./EXPLAIN_ANALYZE.md)