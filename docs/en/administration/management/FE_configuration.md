---
displayed_sidebar: docs
---

import FEConfigMethod from '../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../_assets/commonMarkdown/StaticFE_config_note.mdx'

import EditionSpecificFEItem from '../../_assets/commonMarkdown/Edition_Specific_FE_Item.mdx'

# FE Configuration

<FEConfigMethod />

## View FE configuration items

After your FE is started, you can run the ADMIN SHOW FRONTEND CONFIG command on your MySQL client to check the parameter configurations. If you want to query the configuration of a specific parameter, run the following command:

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

For detailed description of the returned fields, see [`ADMIN SHOW CONFIG`](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md).

:::note
You must have administrator privileges to run cluster administration-related commands.
:::

## Configure FE parameters

### Configure FE dynamic parameters

You can configure or modify the settings of FE dynamic parameters using [`ADMIN SET FRONTEND CONFIG`](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md).

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### Configure FE static parameters

<StaticFEConfigNote />

## Parameter groups

The parameters are grouped in these categories:

- [Logging](./FE_parameters/log_server_meta.md)
- [Server](./FE_parameters/log_server_meta.md)
- [Metadata and Cluster management](./FE_parameters/log_server_meta.md)
- [User, role, and privilege](./FE_parameters/user_query_loading.md)
- [Query engine](./FE_parameters/user_query_loading.md)
- [Loading and unloading](./FE_parameters/user_query_loading.md)
- [Statistic report](./FE_parameters/stats_storage.md)
- [Storage](./FE_parameters/stats_storage.md)
- [Shared-data](./FE_parameters/shared_lake_other.md)
- [Data Lake](./FE_parameters/shared_lake_other.md)
