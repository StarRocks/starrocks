---
displayed_sidebar: docs
---

# ADD BACKEND BLACKLIST

## 説明

BE ノードを BE ブラックリストに追加します。クエリ実行時にノードの使用を禁止するために、手動で BE ノードをブラックリストに追加することができます。これにより、BE ノードへの接続失敗による頻繁なクエリ失敗やその他の予期しない動作を回避できます。

この機能は v3.3.0 以降でサポートされています。詳細については、[Manage BE Blacklist](../../../../administration/management/BE_blacklist.md) を参照してください。

:::note

この操作を実行できるのは、SYSTEM レベルの BLACKLIST 権限を持つユーザーのみです。

:::

デフォルトでは、StarRocks は BE ブラックリストを自動的に管理し、接続が失われた BE ノードをブラックリストに追加し、接続が再確立されたときにブラックリストから削除します。ただし、手動でブラックリストに追加された BE ノードは、StarRocks によってブラックリストから削除されません。

## 構文

```SQL
ADD BACKEND BLACKLIST <be_id>[, ...]
```

## パラメータ

`be_id`: ブラックリストに追加する BE ノードの ID。BE ID は [SHOW BACKENDS](SHOW_BACKENDS.md) を実行して取得できます。

## 例

```SQL
-- BE ID を取得します。
SHOW BACKENDS\G
*************************** 1. row ***************************
            BackendId: 10001
                   IP: xxx.xx.xx.xxx
                   ...
-- BE をブラックリストに追加します。
ADD BACKEND BLACKLIST 10001;
```

## 関連する SQL

- [DELETE BACKEND BLACKLIST](./DELETE_BACKEND_BLACKLIST.md)
- [SHOW BACKEND BLACKLIST](./SHOW_BACKEND_BLACKLIST.md)
- [SHOW BACKENDS](SHOW_BACKENDS.md)