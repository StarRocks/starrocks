---
displayed_sidebar: docs
description: "StarRocks BE 配置参数完整参考：be.conf 或 SQL 命令可配置的所有 BE 参数。"
keywords: ['Canshu']
---

import BEConfigMethod from '../../../_assets/commonMarkdown/BE_config_method.mdx'

import PostBEConfig from '../../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../../_assets/commonMarkdown/StaticBE_config_note.mdx'

import EditionSpecificBEItem from '../../../_assets/commonMarkdown/Edition_Specific_BE_Item.mdx'

# BE 配置项

<BEConfigMethod />

## 查看 BE 配置项

您可以通过以下命令查看 BE 配置项：

```SQL
SELECT * FROM information_schema.be_configs WHERE NAME LIKE "%<name_pattern>%"
```

## 配置 BE 参数

<PostBEConfig />

<StaticBEConfigNote />

## 参数组

参数分为以下几类：

- [日志](./log_server_meta.md)
- [服务器](./log_server_meta.md)
- [元数据与集群管理](./log_server_meta.md)
- [查询引擎](./query_loading.md)
- [导入导出](./query_loading.md)
- [统计报告](./stats_storage.md)
- [存储](./stats_storage.md)
- [存算分离](./shared_lake_other.md)
- [数据湖](./shared_lake_other.md)
- [其他](./shared_lake_other.md)
