---
displayed_sidebar: docs
---

# SHOW PROFILELIST

SHOW PROFILELIST は、StarRocks クラスターにキャッシュされたクエリプロファイルレコードを一覧表示します。クエリプロファイルの詳細については、[Query Profile Overview](../../../../best_practices/query_tuning/query_profile_overview.md) を参照してください。

この機能は v3.1 以降でサポートされています。

この操作を実行するための特別な権限は必要ありません。

## Syntax

```SQL
SHOW PROFILELIST [LIMIT n]
```

## Parameters

`LIMIT n`: 最新の n 件のレコードを一覧表示します。

## Return value

| **Return** | **Description**                                              |
| ---------- | ------------------------------------------------------------ |
| QueryId    | クエリの ID。                                                |
| StartTime  | クエリの開始時間。                                           |
| Time       | クエリのレイテンシー。                                       |
| State      | クエリのステータス。以下を含みます: `Error`: クエリがエラーに遭遇した。`Finished`: クエリが完了した。`Running`: クエリが実行中。 |
| Statement  | クエリのステートメント。                                     |

## Examples

例 1: 最新のクエリプロファイルレコードを 5 件表示します。

```SQL
SHOW PROFILELIST LIMIT 5;
```

## Relevant SQLs

- [ANALYZE PROFILE](./ANALYZE_PROFILE.md)
- [EXPLAIN ANALYZE](./EXPLAIN_ANALYZE.md)