---
displayed_sidebar: docs
sidebar_position: 0.5
---

# 函数

import DocCardList from '@theme/DocCardList';

StarRocks 提供了丰富的函数，方便您在日常数据查询和分析时使用。除了常见的函数分类，StarRocks 也支持 ARRAY、JSON、MAP、STRUCT 等半结构化函数，支持 [Lambda 高阶函数](Lambda_expression.md)。如果以上函数都不符合您的需求，您还可以自行编写 [Java UDF](JAVA_UDF.md) 来满足业务需求。StarRocks 还提供 [Hive Bitmap UDF](hive_bitmap_udf.md) 功能，您可以在 Hive 里计算生成 Bitmap 后，再导入 StarRocks；将 StarRocks 里生成的 Bitmap，导出到 Hive，方便其它系统使用。

<DocCardList />
