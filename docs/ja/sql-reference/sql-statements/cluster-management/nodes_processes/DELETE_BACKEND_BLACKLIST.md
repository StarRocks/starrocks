---
displayed_sidebar: docs
---

# DELETE BACKEND/COMPUTE NOT BLACKLIST

BE/CN ノードを BE ブラックリストから削除します。StarRocks は、ユーザーによって手動でブラックリストに登録された BE/CN ノードを削除しないことに注意してください。

BE ブラックリストは v3.3.0 以降でサポートされ、CN ブラックリストは v4.0 以降でサポートされています。詳細については、[Manage BE and CN Blacklist](../../../../administration/management/BE_blacklist.md) を参照してください。

:::note

この操作を実行できるのは、SYSTEM レベルの BLACKLIST 権限を持つユーザーのみです。

:::

## Syntax

```SQL
DELETE { BACKEND | COMPUTE NODE } BLACKLIST { <be_id>[, ...] | <cn_id>[, ...] }
```

## Parameters

`be_id` または `cn_id`: ブラックリストから削除する BE または CN ノードの ID。ブラックリストに登録された BE/CN の ID は、[SHOW BACKEND/COMPUTE NODE BLACKLIST](./SHOW_BACKEND_BLACKLIST.md) を実行して取得できます。

## Examples

```SQL
DELETE BACKEND BLACKLIST 10001;
```

## Relevant SQLs

- [ADD BACKEND/COMPUTE NODE BLACKLIST](./ADD_BACKEND_BLACKLIST.md)
- [SHOW BACKEND/COMPUTE NODE BLACKLIST](./SHOW_BACKEND_BLACKLIST.md)
- [SHOW BACKENDS](./SHOW_BACKENDS.md)
- [SHOW COMPUTE NODES](./SHOW_COMPUTE_NODES.md)