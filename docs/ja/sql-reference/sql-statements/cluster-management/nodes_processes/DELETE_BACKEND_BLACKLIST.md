---
displayed_sidebar: docs
---

# DELETE BACKEND BLACKLIST

## 説明

BE ノードを BE ブラックリストから削除します。StarRocks は、ユーザーによって手動でブラックリストに登録された BE ノードを削除しないことに注意してください。

この機能は v3.3.0 以降でサポートされています。詳細については、[Manage BE Blacklist](../../../../administration/management/BE_blacklist.md) を参照してください。

:::note

この操作を実行できるのは、SYSTEM レベルの BLACKLIST 権限を持つユーザーのみです。

:::

## 構文

```SQL
DELETE BACKEND BLACKLIST <be_id>[, ...]
```

## パラメータ

`be_id`: ブラックリストから削除する BE ノードの ID。ブラックリストに登録された BE の ID は、[SHOW BACKEND BLACKLIST](./SHOW_BACKEND_BLACKLIST.md) を実行することで取得できます。

## 例

```SQL
DELETE BACKEND BLACKLIST 10001;
```

## 関連 SQL

- [ADD BACKEND BLACKLIST](./ADD_BACKEND_BLACKLIST.md)
- [SHOW BACKEND BLACKLIST](./SHOW_BACKEND_BLACKLIST.md)
- [SHOW BACKENDS](SHOW_BACKENDS.md)