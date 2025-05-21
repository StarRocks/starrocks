---
displayed_sidebar: docs
---

import Beta from '../_assets/commonMarkdown/_beta.mdx'

# SQL 事务

<Beta />

本文介绍如何通过简易的 SQL 事务批量提交多个 INSERT 语句。

## 概述

从 v3.5.0 开始，StarRocks 支持 SQL 事务，用于在将数据导入到多个表时，确保更新操作的原子性。

一个事务由多个 SQL 语句组成，这些语句在同一个原子单元中处理。事务中的所有语句只能同时生效或撤销，从而保证事务的 ACID（原子性、一致性、隔离性、持久性）特性。

目前，StarRocks 中的 SQL 事务仅支持通过 INSERT 进行的导入操作，并且不允许对同一表执行多个 INSERT 语句。事务的 ACID 属性仅在有限的 READ COMMITTED 隔离级别上得到保证，即：

- 每条语句只作用于其开始执行前已提交的数据。
- 如果在两条语句之间有其他事务提交，它们有可能作用于不同的数据。
- 在同一个事务中，先执行的 DML 语句引起的数据变更对后续语句不可见。

事务绑定于单个会话。多个会话无法共享同一个事务。

## 用法

1. 必须通过执行 START TRANSACTION 语句来启动事务。StarRocks 也支持同义词 BEGIN。

   ```SQL
   { START TRANSACTION | BEGIN [ WORK ] }
   ```

2. 启动事务后，可以在事务中定义多个 INSERT 语句。有关详细信息，请参见[使用说明](#使用说明)。

3. 必须通过执行 COMMIT 或 ROLLBACK 显式结束事务。

   - 要应用（提交）事务，请使用以下语法：

     ```SQL
     COMMIT [ WORK ]
     ```

   - 要撤销（回滚）事务，请使用以下语法：

     ```SQL
     ROLLBACK [ WORK ]
     ```

## 示例

```SQL
BEGIN WORK;
INSERT INTO source_wiki_edit
WITH LABEL insert_load_wikipedia
VALUES
    ("2015-09-12 00:00:00","#en.wikipedia","AustinFF",0,0,0,0,0,21,5,0),
    ("2015-09-12 00:00:00","#ca.wikipedia","helloSR",0,1,0,1,0,3,23,0);
INSERT INTO insert_wiki_edit
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "XXXXXXXXXX",
        "aws.s3.secret_key" = "YYYYYYYYYY",
        "aws.s3.region" = "us-west-2"
);
COMMIT WORK;
```

## 使用说明

- 目前，StarRocks 的 SQL 事务仅支持 INSERT 和 SELECT 语句。
- 事务中 DML 语句的所有目标表必须在同一个数据库中。
- 不允许在事务中对同一表执行多个 INSERT 语句。
- 不允许嵌套事务，即不能在一对 BEGIN-COMMIT/ROLLBACK 之间再次指定 BEGIN WORK。
- 如果执行事务的会话被终止或关闭，事务会自动回滚。
- 如上所述，StarRocks 仅支持有限的 READ COMMITTED 事务隔离级别。
- 不支持写冲突检查。当两个事务同时写入同一个表时，两个事务都可以成功提交。数据更改的可见性（顺序）取决于 COMMIT WORK 语句的执行顺序。
