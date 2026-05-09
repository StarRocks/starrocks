---
displayed_sidebar: docs
keywords: ['Canshu']
---

import BEConfigMethod from '../../_assets/commonMarkdown/BE_config_method.mdx'

import PostBEConfig from '../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../_assets/commonMarkdown/StaticBE_config_note.mdx'

import EditionSpecificBEItem from '../../_assets/commonMarkdown/Edition_Specific_BE_Item.mdx'

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

- [日志](./BE_parameters/log_server_meta.md)
- [服务器](./BE_parameters/log_server_meta.md)
- [元数据与集群管理](./BE_parameters/log_server_meta.md)
- [查询引擎](./BE_parameters/query_loading.md)
- [导入导出](./BE_parameters/query_loading.md)
- [统计报告](./BE_parameters/stats_storage.md)
- [存储](./BE_parameters/stats_storage.md)
- [存算分离](./BE_parameters/shared_lake_other.md)
- [数据湖](./BE_parameters/shared_lake_other.md)
- [其他](./BE_parameters/shared_lake_other.md)
