---
displayed_sidebar: docs
---

import BEConfigMethod from '../../_assets/commonMarkdown/BE_config_method.mdx'

import CNConfigMethod from '../../_assets/commonMarkdown/CN_config_method.mdx'

import PostBEConfig from '../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../_assets/commonMarkdown/StaticBE_config_note.mdx'

# BE Configuration

<BEConfigMethod />

<CNConfigMethod />

## View BE configuration items

You can view the BE configuration items using the following command:

```SQL
SELECT * FROM information_schema.be_configs WHERE NAME LIKE "%<name_pattern>%"
```

## Configure BE parameters

<PostBEConfig />

<StaticBEConfigNote />

## Parameter groups

The parameters are grouped in these categories:

- [Logging](./BE_parameters/log_server_meta.md)
- [Server](./BE_parameters/log_server_meta.md)
- [Metadata and Cluster management](./BE_parameters/log_server_meta.md)
- [Query engine](./BE_parameters/query_loading.md)
- [Loading and unloading](./BE_parameters/query_loading.md)
- [Statistic report](./BE_parameters/stats_storage.md)
- [Storage](./BE_parameters/stats_storage.md)
- [Shared-data](./BE_parameters/shared_lake_other.md)
- [Data Lake](./BE_parameters/shared_lake_other.md)
- [Other](./BE_parameters/shared_lake_other.md)
