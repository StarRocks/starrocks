---
displayed_sidebar: docs
---

# ADD BACKEND/COMPUTE NOTE BLACKLIST

BE または CN ノードを BE および CN ブラックリストに追加します。クエリ実行時にノードの使用を禁止するために、手動で BE/CN ノードをブラックリストに追加することができます。これにより、ノードへの接続が失敗したことによる頻繁なクエリの失敗やその他の予期しない動作を回避できます。

BE ブラックリストは v3.3.0 以降でサポートされ、CN ブラックリストは v4.0 以降でサポートされています。詳細については、[Manage BE and CN Blacklist](../../../../administration/management/BE_blacklist.md) を参照してください。

:::note

この操作を実行できるのは、SYSTEM レベルの BLACKLIST 権限を持つユーザーのみです。

:::

デフォルトでは、StarRocks は BE および CN ブラックリストを自動的に管理し、接続が失われた BE/CN ノードをブラックリストに追加し、接続が再確立されたときにブラックリストから削除します。ただし、手動でブラックリストに追加されたノードは、StarRocks によってブラックリストから削除されません。

## Syntax

```SQL
ADD { BACKEND | COMPUTE NODE } BLACKLIST { <be_id>[, ...] | <cn_id>[, ...] }
```

## Parameters

`be_id` または `cn_id`: ブラックリストに追加する BE または CN ノードの ID。BE ID は [SHOW BACKENDS](./SHOW_BACKENDS.md) を実行して取得し、CN ID は [SHOW COMPUTE NODES](./SHOW_COMPUTE_NODES.md) を実行して取得できます。

## Examples

```SQL
-- BE ID を取得します。
SHOW BACKENDS\G
*************************** 1. row ***************************
            BackendId: 10001
                   IP: xxx.xx.xx.xxx
                   ...
-- BE をブラックリストに追加します。
ADD BACKEND BLACKLIST 10001;

-- CN ID を取得します。
SHOW COMPUTE NODES\G
*************************** 1. row ***************************
        ComputeNodeId: 10005
                   IP: xxx.xx.xx.xxx
                   ...
-- CN をブラックリストに追加します。
ADD COMPUTE NODE BLACKLIST 10005;
```

## Relevant SQLs

- [DELETE BACKEND/COMPUTE NODE BLACKLIST](./DELETE_BACKEND_BLACKLIST.md)
- [SHOW BACKEND/COMPUTE NODE BLACKLIST](./SHOW_BACKEND_BLACKLIST.md)
- [SHOW BACKENDS](./SHOW_BACKENDS.md)
- [SHOW COMPUTE NODES](./SHOW_COMPUTE_NODES.md)