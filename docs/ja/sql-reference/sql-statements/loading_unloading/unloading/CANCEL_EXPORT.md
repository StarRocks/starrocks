---
displayed_sidebar: docs
---

# CANCEL EXPORT

## 説明

指定されたデータアンロードジョブをキャンセルします。状態が `CANCELLED` または `FINISHED` のアンロードジョブはキャンセルできません。アンロードジョブのキャンセルは非同期プロセスです。[SHOW EXPORT](SHOW_EXPORT.md) ステートメントを使用して、アンロードジョブが正常にキャンセルされたかどうかを確認できます。アンロードジョブが正常にキャンセルされた場合、`State` の値は `CANCELLED` になります。

CANCEL EXPORT ステートメントを使用するには、指定されたアンロードジョブが属するデータベースに対して、次のいずれかの権限を持っている必要があります: `SELECT_PRIV`, `LOAD_PRIV`, `ALTER_PRIV`, `CREATE_PRIV`, `DROP_PRIV`, および `USAGE_PRIV`。権限の詳細については、[GRANT](../../account-management/GRANT.md) を参照してください。

## 構文

```SQL
CANCEL EXPORT
[FROM db_name]
WHERE QUERYID = "query_id"
```

## パラメータ

| **パラメータ** | **必須** | **説明** |
| ------------- | -------- | -------- |
| db_name       | いいえ   | アンロードジョブが属するデータベースの名前。このパラメータが指定されていない場合、現在のデータベース内のアンロードジョブがキャンセルされます。 |
| query_id      | はい     | アンロードジョブのクエリID。[LAST_QUERY_ID()](../../../sql-functions/utility-functions/last_query_id.md) 関数を使用してIDを取得できます。この関数は最新のクエリIDのみを返すことに注意してください。 |

## 例

例 1: 現在のデータベースでクエリIDが `921d8f80-7c9d-11eb-9342-acde48001121` のアンロードジョブをキャンセルします。

```SQL
CANCEL EXPORT
WHERE queryid = "921d8f80-7c9d-11eb-9342-acde48001121";
```

例 2: `example_db` データベースでクエリIDが `921d8f80-7c9d-11eb-9342-acde48001121` のアンロードジョブをキャンセルします。

```SQL
CANCEL EXPORT 
FROM example_db 
WHERE queryid = "921d8f80-7c9d-11eb-9342-acde48001122";
```