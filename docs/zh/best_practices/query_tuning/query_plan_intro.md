---
displayed_sidebar: docs
sidebar_position: 10
keywords: ['cha xun']
---

# 查询调优简介

查询调优对于在 StarRocks 中实现高性能和高可靠性至关重要。本目录汇集了实用指南、参考资料和可操作的方案，帮助您在每个阶段分析、诊断和优化查询性能——从编写 SQL 到解释执行细节。

在 StarRocks 中，有效的查询调优通常遵循自上而下的过程：

1. **识别问题**  
   - 检测慢查询、高资源使用或意外结果。  
   - 在 StarRocks 中，利用内置的监控工具、查询历史和审计日志快速识别问题查询或异常模式。  
   - 参见：**[查询调优方案](./query_profile_tuning_recipes.md)** 进行症状驱动的诊断，和 **[查询概况概览](./query_profile_overview.md)** 访问查询历史和概况。

2. **收集和分析执行信息**  
   - 使用 `EXPLAIN` 或 `EXPLAIN ANALYZE` 获取查询计划。  
   - 启用并查看 Query Profile 以收集详细的执行指标。  
   - 参见：**[查询计划概览](./query_planning.md)** 理解查询计划，**[Explain Analyze & 基于文本的概况分析](./query_profile_text_based_analysis.md)** 进行逐步分析，以及 **[查询概况概览](./query_profile_overview.md)** 启用和解释概况。

3. **定位根本原因**  
   - 确定哪个阶段或 Operator 消耗了最多的时间或资源。  
   - 检查常见问题：次优的连接顺序、缺失的索引、数据分布问题或低效的 SQL 模式。  
   - 参见：**[查询概况指标](./query_profile_operator_metrics.md)** 获取指标和 Operator 的词汇表，以及 **[查询调优方案](./query_profile_tuning_recipes.md)** 进行根本原因分析。

4. **应用调优策略**  
   - SQL 重写：重写或优化 SQL 查询（例如，添加过滤器，避免使用 SELECT *）。  
   - 模型调优：添加索引、更改表模型、分区、聚簇。  
   - 查询计划调优：如有必要，使用提示或变量引导优化器。  
   - 执行调优：针对特定工作负载调整会话变量。  
   - 参见：**[模型调优方案](./schema_tuning.md)** 进行模型级别优化，**[查询提示](./query_hint.md)** 获取优化器提示，以及 **[查询调优方案](./query_profile_tuning_recipes.md)** 进行计划调优和执行调优。

5. **验证和迭代**  
   - 重新运行查询并比较更改前后的性能。  
   - 查看新的查询计划和概况以确保改进。  
   - 根据需要重复该过程以进一步优化。

无论您是 DBA、开发人员还是数据工程师，这些资源将帮助您：
- 诊断和解决慢速或资源密集型查询
- 理解优化器选择和执行细节
- 应用最佳实践和高级调优策略

从概览开始，根据需要深入参考资料，并使用方案和技巧解决 StarRocks 中的实际性能挑战。
