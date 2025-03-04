---
displayed_sidebar: docs
---

# DROP RESOURCE GROUP

## 説明

指定されたリソースグループを削除します。

:::tip

この操作には、対象のリソースグループに対する DROP 権限が必要です。この権限を付与するには、[GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

## 構文

```SQL
DROP RESOURCE GROUP <resource_group_name>
```

## パラメーター

| **パラメーター**    | **説明**                                   |
| ------------------- | ----------------------------------------- |
| resource_group_name | 削除するリソースグループの名前です。       |

## 例

例 1: リソースグループ `rg1` を削除します。

```SQL
DROP RESOURCE GROUP rg1;
```