---
sidebar_position: 1
sidebar_label: '概述'
keywords: ['nei cun', 'S3 API', 'reduce cost', 'efficiency', 'efficient', 'performance']
---

# 最佳实践

这些最佳实践由经验丰富的数据库工程师撰写。高效设计不仅能提高查询速度，还能通过减少存储、CPU 和对象存储（如 S3）API 成本来降低费用。

## 一般表设计

三个指南涵盖：

- [分区](./partitioning.md)
- [聚簇](./table_clustering.md)
- [分桶](./bucketing.md)

了解：

- 分区和分桶之间的区别
- 何时进行分区
- 如何选择高效的排序键
- 在哈希分桶和随机分桶之间进行选择

## 主键表

[主键](./primarykey_table.md)表使用StarRocks设计的新存储引擎。其主要优势在于支持实时数据更新，同时确保复杂临时查询的高效性能。在实时业务分析中，决策可以从主键表中受益，这些表使用最新数据进行实时结果分析，减轻数据分析中的数据延迟。

了解：

- 选择主键索引的类型
- 选择主键
- 监控和管理内存使用
- 调优
