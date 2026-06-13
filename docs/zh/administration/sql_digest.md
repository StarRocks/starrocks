---
displayed_sidebar: docs
description: "SQL Digest fingerprints user SQL statements by removing parameters to group and analyze SQL queries with identical structure for performance insights."
---

# SQL Digest

本文介绍 StarRocks 的 SQL Digest 功能。该功能从 v3.3.6 开始支持。

## 概述

SQL Digest 是指将用户 SQL 语句中的参数去除后生成的指纹，用于将结构相同但参数不同的 SQL 聚类。常见的使用场景包括：

- 查找查询历史中结构相同但参数不同的其他 SQL
- 统计相同结构的 SQL 执行频次、累积耗时等信息
- 分析系统中耗时最高的 SQL 类型

在 StarRocks 中，SQL Digest 主要通过审计日志 **fe.audit.log** 记录。例如，执行以下两条 SQL 语句：

```SQL
SELECT count(*) FROM lineorder WHERE lo_orderdate > '19920101';
SELECT count(*) FROM lineorder WHERE lo_orderdate > '19920202';
```

在 **fe.audit.log** 中将会生成相同的 Digest。

```SQL
Digest=f58bb71850d112014f773717830e7f77
Digest=f58bb71850d112014f773717830e7f77
```

## 使用方法

您需要通过设置FE配置项 `enable_sql_digest` 为 `true` 来启用 SQL Digest 功能。

您可以通过以下命令动态启用此功能：

```SQL
ADMIN SET FRONTEND CONFIG ("enable_sql_digest" = "true");
```

要永久启用此功能，需在 FE 配置文件 **fe.conf** 中添加 `enable_sql_digest = true` 并重启 FE。

启用后，可借助 [AuditLoader](./management/audit_loader.md) 插件对 SQL 进行统计和分析。

### 查询相似 SQL

```SQL
SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__ 
WHERE digest = '<Digest>'
LIMIT 1;
```

### 统计相似 SQL 每日执行次数和耗时

```SQL
SELECT 
    date_trunc('day', `timestamp`) query_date, 
    count(*), 
    sum(queryTime), 
    sum(scanRows), 
    sum(cpuCostNs), 
    sum(memCostBytes)
FROM starrocks_audit_db__.starrocks_audit_tbl__ 
WHERE digest = '<Digest>'
GROUP BY query_date
ORDER BY query_date 
DESC LIMIT 30;
```

### 统计相似 SQL 的平均执行耗时

```SQL
SELECT avg(queryTime), min(queryTime), max(queryTime), stddev(queryTime)
FROM starrocks_audit_db__.starrocks_audit_tbl__ 
WHERE digest = '<Digest>';
```

### 聚合相似 SQL，分析耗时最高的 SQL 类型

```SQL
WITH top_sql AS (
    SELECT digest, sum(queryTime)
    FROM starrocks_audit_db__.starrocks_audit_tbl__ 
    GROUP BY digest
    ORDER BY sum(queryTime) 
    DESC LIMIT 10 
)
SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__ 
WHERE digest IN (SELECT digest FROM top_sql);
```

## 参数化规则

- SQL 中的常量值会被归一化。例如，包含 `WHERE a = 1` 和 `WHERE a = 2` 的相同类型 SQL 会生成相同的 Digest。
- 对于 IN 谓词会进行归一化。例如，包含 `IN (1,2,3)` 和 `IN (1,2)` 的相同类型 SQL 会生成相同的 Digest。
- 对于 `LIMIT N` 会进行归一化。例如，包含 `LIMIT 10` 和 `LIMIT 30` 的相同类型 SQL 会生成相同的 Digest。

## 跨库 Digest

默认情况下，SQL Digest 的计算包含数据库名。这意味着相同模式的 SQL 在不同数据库中执行会产生不同的 Digest。例如：

```SQL
USE db1;
SELECT * FROM t WHERE id = 1;
-- Digest: aaa...

USE db2;
SELECT * FROM t WHERE id = 1;
-- Digest: bbb...（与上面不同）
```

如果希望相同模式的 SQL 无论在哪个数据库执行都产生相同的 Digest，可以设置会话变量 `sql_digest_exclude_db` 为 `true`：

```SQL
SET sql_digest_exclude_db = true;
```

启用后，数据库名将从 Digest 计算中排除，从而支持跨库聚合相同模式的 SQL。

```SQL
USE db1;
SELECT * FROM t WHERE id = 1;
-- Digest: ccc...

USE db2;
SELECT * FROM t WHERE id = 1;
-- Digest: ccc...（与上面相同）
```

:::note

无论是否启用此选项，外部 Catalog 名称始终会包含在 Digest 中。

:::
