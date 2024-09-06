---
displayed_sidebar: docs
---

# DROP RESOURCE GROUP

## 功能

删除指定资源组。

:::tip

该操作需要对应 Resource Group 的 DROP 权限。请参考 [GRANT](../../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```SQL
DROP RESOURCE GROUP <resource_group_name>
```

## 参数说明

| **参数**            | **说明**         |
| ------------------- | ---------------- |
| resource_group_name | 待删除资源组名。 |

## 示例

示例一：删除资源组 `rg1`。

```SQL
DROP RESOURCE GROUP rg1;
```
