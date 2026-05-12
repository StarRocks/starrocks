---
displayed_sidebar: docs
---

import FEConfigMethod from '../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../_assets/commonMarkdown/StaticFE_config_note.mdx'

import EditionSpecificFEItem from '../../_assets/commonMarkdown/Edition_Specific_FE_Item.mdx'

# FE 配置

<FEConfigMethod />

## 查看 FE 配置项

FE 启动后，您可以在 MySQL 客户端运行 ADMIN SHOW FRONTEND CONFIG 命令查看参数配置。如果要查询特定参数的配置，请运行以下命令：

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

有关返回字段的详细说明，请参阅 [`ADMIN SHOW CONFIG`](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md)。

:::note
您必须具有管理员权限才能运行集群管理相关命令。
:::

## 配置 FE 参数

### 配置 FE 动态参数

您可以使用 [`ADMIN SET FRONTEND CONFIG`](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) 命令配置或修改 FE 动态参数。

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### 配置 FE 静态参数

<StaticFEConfigNote />

## 参数组

参数分为以下几类：

- [日志](./FE_parameters/log_server_meta.md)
- [服务器](./FE_parameters/log_server_meta.md)
- [元数据和集群管理](./FE_parameters/log_server_meta.md)
- [用户、角色和权限](./FE_parameters/user_query_loading.md)
- [查询引擎](./FE_parameters/user_query_loading.md)
- [加载与卸载](./FE_parameters/user_query_loading.md)
- [统计报告](./FE_parameters/stats_storage.md)
- [存储](./FE_parameters/stats_storage.md)
- [存算分离](./FE_parameters/shared_lake_other.md)
- [数据湖](./FE_parameters/shared_lake_other.md)
- [其他](./FE_parameters/shared_lake_other.md)
