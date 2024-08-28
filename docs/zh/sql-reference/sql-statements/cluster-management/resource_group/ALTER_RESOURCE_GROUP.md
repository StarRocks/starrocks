---
keywords: ['xiugai'] 
displayed_sidebar: docs
---

# ALTER RESOURCE GROUP

## 功能

修改资源组设置。

:::tip

该操作需要对应 Resource Group 的 ALTER 权限。请参考 [GRANT](../../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```SQL
ALTER RESOURCE GROUP <resource_group_name>
{  ADD CLASSIFIER1, CLASSIFIER2, ...
 | DROP (CLASSIFIER_ID_1, CLASSIFIER_ID_2, ...)
 | DROP ALL
 | WITH resource_limit 
}
```

## 参数说明

| **参数**            | **说明**                                                     |
| ------------------- | ------------------------------------------------------------ |
| resource_group_name | 待修改的资源组名。                                           |
| ADD                 | 为资源组添加分类器。关于分类器参数具体信息，请见 [CREATE RESOURCE GROUP - 参数说明](CREATE_RESOURCE_GROUP.md)。 |
| DROP                | 通过分类器 ID 为资源组删除对应分类器。您可以通过 [SHOW RESOURCE GROUP](SHOW_RESOURCE_GROUP.md) 语句查看分类器 ID。 |
| DROP ALL            | 为资源组删除所有分类器。                                     |
| WITH                | 为资源组修改资源限制。关于资源限制参数具体信息，请见 [CREATE RESOURCE GROUP - 参数说明](CREATE_RESOURCE_GROUP.md)。 |

## 示例

示例一：为资源组 `rg1` 添加新的分类器。

```SQL
ALTER RESOURCE GROUP rg1 ADD (user='root', query_type in ('select'));
```

示例二：为资源组 `rg1` 删除 ID 为 `300040`，`300041` 以及 `300041` 的分类器。

```SQL
ALTER RESOURCE GROUP rg1 DROP (300040, 300041, 300041);
```

示例三：为资源组 `rg1` 删除所有分类器。

```SQL
ALTER RESOURCE GROUP rg1 DROP ALL;
```

示例四：修改资源组 `rg1` 的资源限制。

```SQL
ALTER RESOURCE GROUP rg1 WITH (
    'cpu_core_limit' = '20'
);
```
