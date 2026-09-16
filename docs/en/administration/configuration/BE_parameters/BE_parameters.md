---
displayed_sidebar: docs
description: "StarRocks BE configuration reference: complete list of BE parameters configurable in be.conf or via SQL."
---

import BEConfigMethod from '../../../_assets/commonMarkdown/BE_config_method.mdx'

import CNConfigMethod from '../../../_assets/commonMarkdown/CN_config_method.mdx'

import PostBEConfig from '../../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../../_assets/commonMarkdown/StaticBE_config_note.mdx'

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

- [Logging](./log_server_meta.md)
- [Server](./log_server_meta.md)
- [Metadata and Cluster management](./log_server_meta.md)
- [Query engine](./query_loading.md)
- [Loading and unloading](./query_loading.md)
- [Statistic report](./stats_storage.md)
- [Storage](./stats_storage.md)
- [Shared-data](./shared_lake_other.md)
- [Data Lake](./shared_lake_other.md)
- [Other](./shared_lake_other.md)
