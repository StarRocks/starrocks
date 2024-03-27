---
displayed_sidebar: "Chinese"
---

# StarRocks

StarRocks 是一款高性能分析型数据仓库，使用向量化、MPP 架构、CBO、智能物化视图、可实时更新的列式存储引擎等技术实现多维、实时、高并发的数据分析。StarRocks 既支持从各类实时和离线的数据源高效导入数据，也支持直接分析数据湖上各种格式的数据。StarRocks 兼容 MySQL 协议，可使用 MySQL 客户端和常用 BI 工具对接。同时 StarRocks 具备水平扩展，高可用、高可靠、易运维等特性。广泛应用于实时数仓、OLAP 报表、数据湖分析等场景。

[StarRocks](https://github.com/StarRocks/starrocks/tree/main) 是 Linux 基金会项目，采用 Apache 2.0 许可证，可在 StarRocks GitHub 存储库中找到（请参阅 [StarRocks 许可证](https://github.com/StarRocks/starrocks/blob/main/LICENSE.txt)）。StarRocks（i）链接或调用第三方软件库中的函数，其许可证可在 [licenses-binary](https://github.com/StarRocks/starrocks/tree/main/licenses-binary) 文件夹中找到；和（ii）包含第三方软件代码，其许可证可在 [licenses](https://github.com/StarRocks/starrocks/tree/main/licenses) 文件夹中找到。

---

## Popular topics

### 产品简介

[OLAP、特性、系统架构](../introduction/introduction.mdx)

### 快速入门信息

[快速部署、导入、查询](../quick_start/quick_start.mdx)

### 导入数据

[数据清洗、转换、导入](../loading/Loading_intro.md)

### 表设计

[表、索引、分区、加速](../table_design/StarRocks_table_design.md)

### 查询数据湖

[Iceberg, Hive, Delta Lake, …](../data_source/data_lakes.mdx)

### 半结构化类型

[JSON, map, struct, array](../sql-reference/sql-statements/data-types/JSON.md)

### 外部系统集成

[BI、IDE、认证信息](../integrations/integrations.mdx)

### 管理手册

[扩缩容、备份恢复、权限、性能调优](../administration/administration.mdx)

### 参考手册

[SQL 语法、命令、函数、变量](../reference/reference.mdx)

### 常见问题解答

[部署、导入、查询、权限常见问题](../faq/faq.mdx)

### 性能测试

[性能测试：数据库性能对比](../benchmarking/benchmarking.mdx)