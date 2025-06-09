---
displayed_sidebar: docs
---

# SHOW PROFILELIST

## Description

StarRocks クラスターにキャッシュされているクエリプロファイルレコードを一覧表示します。クエリプロファイルの詳細については、 [Query Profile Overview](../../../../administration/query_profile_overview.md) を参照してください。

この機能は v3.1 以降でサポートされています。

この操作を実行するための特権は必要ありません。

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
| State      | クエリのステータス。`Error`: クエリがエラーに遭遇した場合。`Finished`: クエリが完了した場合。`Running`: クエリが実行中の場合。 |
| Statement  | クエリのステートメント。                                     |

## Examples

Example 1: 最新の 5 件のクエリプロファイルレコードを表示します。

```SQL
SHOW PROFILELIST LIMIT 5;
```

## Relevant SQLs

- [ANALYZE PROFILE](./ANALYZE_PROFILE.md)
- [EXPLAIN ANALYZE](./EXPLAIN_ANALYZE.md)