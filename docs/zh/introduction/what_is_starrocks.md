---
displayed_sidebar: "Chinese"
---

# 什么是 StarRocks

StarRocks 是**新一代极速全场景 MPP (Massively Parallel Processing) 数据库**。StarRocks 的愿景是能够让用户的**数据分析变得更加简单和敏捷**。用户无需经过复杂的预处理，就可以用 StarRocks 来支持多种数据分析场景的极速分析。

StarRocks **架构简洁**，采用了全面向量化引擎，并配备全新设计的 CBO (Cost Based Optimizer) 优化器，**查询速度（尤其是多表关联查询）远超同类产品**。

StarRocks 能很好地支持实时数据分析，并能实现对实时更新数据的高效查询。StarRocks 还支持现代化物化视图，进一步加速查询。

使用 StarRocks，用户可以灵活构建包括大宽表、星型模型、雪花模型在内的各类模型。

StarRocks 兼容 MySQL 协议，支持标准 SQL 语法，易于对接使用，全系统无外部依赖，高可用，易于运维管理。StarRocks 还兼容多种主流 BI 产品，包括 Tableau、Power BI、FineBI 和 Smartbi。

## 适用场景

StarRocks 可以满足企业级用户的多种分析需求，包括 OLAP (Online Analytical Processing) 多维分析、定制报表、实时数据分析和 Ad-hoc 数据分析等。

### OLAP 多维分析

利用 StarRocks 的 MPP 框架和向量化执行引擎，用户可以灵活的选择雪花模型，星型模型，宽表模型或者预聚合模型。适用于灵活配置的多维分析报表，业务场景包括：

- 用户行为分析

- 用户画像、标签分析、圈人

- 高维业务指标报表

- 自助式报表平台

- 业务问题探查分析

- 跨主题业务分析

- 财务报表

- 系统监控分析

### 实时数据仓库

StarRocks 设计和实现了主键表，能够实时更新数据并极速查询，可以秒级同步 TP (Transaction Processing) 数据库的变化，构建实时数仓，业务场景包括：

- 电商大促数据分析

- 物流行业的运单分析

- 金融行业绩效分析、指标计算

- 直播质量分析

- 广告投放分析

- 管理驾驶舱

- 探针分析APM（Application Performance Management）

### 高并发查询

StarRocks 通过良好的数据分布特性，灵活的索引以及物化视图等特性，可以解决面向用户侧的分析场景，业务场景包括：

- 广告主报表分析

- 零售行业渠道人员分析

- SaaS 行业面向用户分析报表

- Dashboard 多页面分析

### 统一分析

- 通过使用一套系统解决多维分析、高并发查询、预计算、实时分析查询等场景，降低系统复杂度和多技术栈开发与维护成本。

- 使用 StarRocks 统一管理数据湖和数据仓库，将高并发和实时性要求很高的业务放在 StarRocks 中分析，也可以使用 External Catalog 和外部表进行数据湖上的分析。
