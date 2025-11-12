---
displayed_sidebar: docs
---

import Beta from '../_assets/commonMarkdown/_beta.mdx'

# SQL 事务

<Beta />

通过简易的 SQL 事务批量提交多个 INSERT 语句。

## 概述

从 v3.5.0 开始，StarRocks 支持 SQL 事务，以确保在操作多张表中的数据时，确保更新操作的原子性。

一个事务由多个 SQL 语句组成，这些语句在同一个原子单元中处理。事务中的所有语句只能同时生效或撤销，从而保证事务的 ACID（原子性、一致性、隔离性、持久性）特性。

目前，StarRocks 的 SQL 事务支持以下操作：
- INSERT INTO
- UPDATE
- DELETE

:::note

- 当前不支持 INSERT OVERWRITE。
- 在同一事务内对同一表执行多个 INSERT 语句的操作仅在 v4.0 及更高版本的存算分离集群中支持。
- UPDATE 和 DELETE 操作仅在 v4.0 及更高版本的存算分离集群中支持。

:::

从 v4.0 起，在单个 SQL 事务内：
- 支持对单张表执行**多个 INSERT 语句**。
- 每张表仅允许执行**一个 UPDATE 或 DELETE 语句**。
- **禁止**在同一张表的 INSERT 语句**之后**执行 UPDATE 或 DELETE 语句。

事务的 ACID 属性仅在有限的 READ COMMITTED 隔离级别上得到保证，即：
- 每条语句只作用于其开始执行前已提交的数据。
- 如果在两条语句之间有其他事务提交，它们有可能作用于不同的数据。
- 在同一个事务中，先执行的 DML 语句引起的数据变更对后续语句不可见。

事务绑定于单个会话。多个会话无法共享同一个事务。

## 用法

1. 必须通过执行 START TRANSACTION 语句来启动事务。StarRocks 也支持同义词 BEGIN。

   ```SQL
   { START TRANSACTION | BEGIN [ WORK ] }
   ```

2. 启动事务后，可以在事务中定义多个 DML 语句。有关详细信息，请参见[使用说明](#使用说明)。

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

1. 在存算分离集群中创建演示表 `desT`，并将数据导入到其中。

    :::note
    若要在存算一体集群中尝试此示例，必须跳过步骤 3，并在步骤 4 中仅定义一条 INSERT 语句。
    :::

    ```SQL
    CREATE TABLE desT (
        k int,
        v int
    ) PRIMARY KEY(k);

    INSERT INTO desT VALUES
    (1,1),
    (2,2),
    (3,3);
    ```

2. 启动事务

    ```SQL
    START TRANSACTION;
    ```

    或

    ```SQL
    BEGIN WORK;
    ```

3. 定义一个 UPDATE 或 DELETE 语句。

    ```SQL
    UPDATE desT SET v = v + 1 WHERE k = 1,
    ```

    或

    ```SQL
    DELETE FROM desT WHERE k = 1;
    ```

4. 定义多个 INSERT 语句。

    ```SQL
    -- 插入具有指定值的数据。
    INSERT INTO desT VALUES (4,4);
    -- 从一个本地表向另一个本地表中插入数据。
    INSERT INTO desT SELECT * FROM srcT;
    -- 从远端存储中插入数据。
    INSERT INTO desT
        SELECT * FROM FILES(
            "path" = "s3://inserttest/parquet/srcT.parquet",
            "format" = "parquet",
            "aws.s3.access_key" = "XXXXXXXXXX",
            "aws.s3.secret_key" = "YYYYYYYYYY",
            "aws.s3.region" = "us-west-2"
    );
    ```

5. 应用或撤销事务。

    - 应用事务中 SQL 语句。

      ```SQL
      COMMIT WORK;
      ```

    - 撤销事务中 SQL 语句。

      ```SQL
      ROLLBACK WORK;
      ```

## 使用说明

- 目前，StarRocks 支持在 SQL 事务中执行 SELECT、INSERT、UPDATE 和 DELETE 语句。UPDATE 和 DELETE 操作仅在 v4.0 及更高版本的存算分离集群中支持。
- 不允许对同一事务中数据已被修改的表执行 SELECT 语句。
- 在同一事务内对同一表执行多个 INSERT 语句的操作仅在 v4.0 及更高版本的存算分离集群中支持。
- 在单个事务内，每张表仅可定义一条 UPDATE 或 DELETE 语句，且必须位于 INSERT 语句之前。
- 后续的 DML 语句无法读取同一事务中先前语句带来的未提交变更。例如，先前 INSERT 语句的目标表不能作为后续语句的源表，否则系统将返回错误。
- 事务中所有 DML 语句的目标表必须位于同一数据库内，不支持跨数据库操作。
- 当前不支持 INSERT OVERWRITE 操作。
- 不允许嵌套事务，即不能在一对 BEGIN-COMMIT/ROLLBACK 之间再次指定 BEGIN WORK。
- 如果执行事务的会话被终止或关闭，事务会自动回滚。
- 如上所述，StarRocks 仅支持有限的 READ COMMITTED 事务隔离级别。
- 不支持写冲突检查。当两个事务同时写入同一个表时，两个事务都可以成功提交。数据更改的可见性（顺序）取决于 COMMIT WORK 语句的执行顺序。
