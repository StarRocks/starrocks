---
displayed_sidebar: docs
sidebar_position: 120
---

# 加速基数保留 Join

本文描述了如何通过表裁剪功能加速基数保留 Join。该功能自 v3.1 开始支持。

## 概述

基数保留 Join（Cardinality-preserving join）是指输出行的基数和重复因子与 Join 内部其中一个输入表的基数和重复因子相同的 Join。例如：

- **Inner Join**

  ```SQL
  SELECT A.* FROM A INNER JOIN B ON A.fk = B.pk;
  ```

  在以上示例中，`A.fk`（外键，Foreign Key）**不为 NULL**，并且引用 `B.pk`（主键，Primary Key）。`A` 中的每一行都恰好与 `B` 中的一行匹配，因此输出的基数和重复因子与 `A` 的相同。

- **Left Join**

  ```SQL
  SELECT A.* FROM A LEFT JOIN B ON A.fk = B.pk;
  ```

  此处 `A.fk` 引用 `B.pk`，但 `A.fk` 可以包含 NULL 值。`A` 中的每一行最多与 `B` 中的一行匹配。因此，输出的基数和重复因子与 `A` 保持一致。

<br />

在此类 Join 中，如果最终输出列仅依赖于表 `A` 的列，表 `B` 的列未被使用，则可以从 Join 中裁剪表 `B`。从 v3.1 开始，StarRocks 支持在基数保留 Join 中进行**表裁剪**，包括 CTE、逻辑视图和子查询。

### 用例：风险控制中的实时特征选择

基数保留 Join 的表裁剪功能在风险控制中的**实时特征选择**等场景中尤其有用。在这种情况下，用户需要从大量表中选择数据，通常还需要处理列和表的组合爆炸。风险控制领域常见的特点包括：

- 大量特征分布在许多独立更新的表中。
- 必须实时查看和查询最新数据。
- 使用**扁平逻辑视图**来简化数据模型，使列提取的 SQL 更简洁和高效。

使用扁平逻辑视图而非其他加速数据层，可以帮助用户高效地访问实时数据。在每个列提取查询中，只需要连接逻辑视图中的少量表，而非所有表。通过从这些查询中裁剪未使用的表，可以减少 Join 数量并提高性能。

### 功能支持

表裁剪功能支持**星型模型**和**雪花模型**中的多表连接，包括在 CTE、逻辑视图和子查询中，实现更高查询执行效率。

目前，表裁剪功能仅支持 OLAP 表和云原生表。多表连接中的外表不能被裁剪。

## 使用方法

以下示例使用 TPC-H 数据集。

### 前提条件

要使用表裁剪功能，必须满足以下条件：

1. 启用表裁剪
2. 设置键约束

#### 启用表裁剪

系统默认禁用表裁剪。您需要通过配置以下 Session 变量启用此功能：

```SQL
-- 启用 RBO 阶段表裁剪。
SET enable_rbo_table_prune=true;
-- 启用 CBO 阶段表裁剪。
SET enable_cbo_table_prune=true; 
-- 为主键表上的 UPDATE 语句启用 RBO 阶段表裁剪。
SET enable_table_prune_on_update = true;
```

#### 设置键约束

被裁剪的表必须在 LEFT Join 或 RIGHT Join 中至少有唯一键（Unique Key）或主键（Primary Key）约束。要在 INNER Join 中裁剪表，必须在唯一键或主键约束之外定义外键（Foreign Key）约束。

主键表和更新表中天然包含其隐式主键或唯一键约束。但是，对于明细表，必须手动定义唯一键约束，并确保不存在重复行。请注意，StarRocks 不会强制检查明细表的唯一键约束，仅将其视为优化提示，用于触发更激进的查询计划。

示例：

```SQL
-- 在创建表时定义唯一键约束。
CREATE TABLE `lineitem` (
  `l_orderkey` int(11) NOT NULL COMMENT "",
  `l_partkey` int(11) NOT NULL COMMENT "",
  `l_suppkey` int(11) NOT NULL COMMENT "",
  `l_linenumber` int(11) NOT NULL COMMENT "",
  `l_quantity` decimal64(15, 2) NOT NULL COMMENT "",
  `l_extendedprice` decimal64(15, 2) NOT NULL COMMENT "",
  `l_discount` decimal64(15, 2) NOT NULL COMMENT "",
  `l_tax` decimal64(15, 2) NOT NULL COMMENT "",
  `l_returnflag` varchar(1) NOT NULL COMMENT "",
  `l_linestatus` varchar(1) NOT NULL COMMENT "",
  `l_shipdate` date NOT NULL COMMENT "",
  `l_commitdate` date NOT NULL COMMENT "",
  `l_receiptdate` date NOT NULL COMMENT "",
  `l_shipinstruct` varchar(25) NOT NULL COMMENT "",
  `l_shipmode` varchar(10) NOT NULL COMMENT "",
  `l_comment` varchar(44) NOT NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`l_orderkey`,`l_partkey`, `l_suppkey`)
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96 
PROPERTIES (
"unique_constraints" = "l_orderkey,l_linenumber"
); 

-- 或者可以在创建表后定义唯一键约束。
ALTER TABLE lineitem SET ("unique_constraints" = "l_orderkey,l_linenumber");
```

另一方面，外键约束必须显式定义。与明细表上的唯一键约束类似，外键约束也被用于优化器的提示。StarRocks 不强制检查外键约束一致性。您必须在将数据生成和导入 StarRocks 时确保数据完整性。

示例：

```SQL
-- 创建在外键约束中引用的表。请注意，被引用的列必须具有唯一键或主键约束。
-- 在此示例中，p_partkey 是表 part 的主键。
CREATE TABLE part (
    p_partkey     int(11) NOT NULL,
    p_name        VARCHAR(55) NOT NULL,
    p_mfgr        CHAR(25) NOT NULL,
    p_brand       CHAR(10) NOT NULL,
    p_type        VARCHAR(25) NOT NULL,
    p_size        INT NOT NULL,
    p_container   CHAR(10) NOT NULL,
    p_retailprice DOUBLE NOT NULL,
    p_comment     VARCHAR(23) NOT NULL
) ENGINE=OLAP
PRIMARY KEY(`p_partkey`)
DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 12;

-- 在创建表时定义外键约束。
CREATE TABLE `lineitem` (
  `l_orderkey` int(11) NOT NULL COMMENT "",
  `l_partkey` int(11) NOT NULL COMMENT "",
  `l_suppkey` int(11) NOT NULL COMMENT "",
  `l_linenumber` int(11) NOT NULL COMMENT "",
  `l_quantity` decimal64(15, 2) NOT NULL COMMENT "",
  `l_extendedprice` decimal64(15, 2) NOT NULL COMMENT "",
  `l_discount` decimal64(15, 2) NOT NULL COMMENT "",
  `l_tax` decimal64(15, 2) NOT NULL COMMENT "",
  `l_returnflag` varchar(1) NOT NULL COMMENT "",
  `l_linestatus` varchar(1) NOT NULL COMMENT "",
  `l_shipdate` date NOT NULL COMMENT "",
  `l_commitdate` date NOT NULL COMMENT "",
  `l_receiptdate` date NOT NULL COMMENT "",
  `l_shipinstruct` varchar(25) NOT NULL COMMENT "",
  `l_shipmode` varchar(10) NOT NULL COMMENT "",
  `l_comment` varchar(44) NOT NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATEK KEY(`l_orderkey`,`l_partkey`, `l_suppkey`)
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96 
PROPERTIES (
"foreign_key_constraints" =  "(l_partkey) REFERENCES part(p_partkey)"
);

-- 或者可以在创建表后定义外键约束。
ALTER TABLE lineitem SET ("foreign_key_constraints" =  "(l_partkey) REFERENCES part(p_partkey)");
```

### 基于唯一键或主键的 LEFT/RIGHT JOIN 表裁剪

在 LEFT JOIN 或 RIGHT JOIN 中进行表裁剪，系统不要求被保留表的外键引用被裁剪表。因此即使无法保证引用完整性，也可使得裁剪更灵活和稳健。

相较于基于外键的 INNER JOIN，基于唯一键或主键的 LEFT/RIGHT JOIN 表裁剪的要求更少。

裁剪的条件如下：

- **被裁剪侧**

  被裁剪表必须位于 LEFT JOIN 中的**右侧**或 RIGHT JOIN 中的**左侧**。

- **连接条件**

  连接必须且只能使用等值条件 (`=`)，并且被裁剪侧的连接列必须是唯一键或主键的**超集**。

- **输出列**

  仅应输出保留侧的列，并且结果应保持与保留侧**相同的基数和重复因子**。

- **NULL****/Default Values**

  保留侧的连接列**可以**包含 NULL 或其他不匹配裁剪侧的默认值。

示例：

1. 创建表并插入数据。

    ```SQL
    -- 表 depts 在列 deptno 上有主键约束。
    CREATE TABLE `depts` (
      `deptno` int(11) NOT NULL COMMENT "",
      `name` varchar(25) NOT NULL COMMENT ""
    ) ENGINE=OLAP
    PRIMARY KEY(`deptno`)
    DISTRIBUTED BY HASH(`deptno`) BUCKETS 10;
    
    CREATE TABLE `emps` (
      `empid` int(11) NOT NULL COMMENT "",
      `deptno` int(11) NOT NULL COMMENT "",
      `name` varchar(25) NOT NULL COMMENT "",
      `salary` double NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`empid`)
    DISTRIBUTED BY HASH(`empid`) BUCKETS 10;
    
    INSERT INTO depts VALUES
    (1, "R&D"),
    (2, "Marketing"), 
    (3, "Community"),
    (4, "DBA"),(5, "POC");
    
    INSERT INTO emps VALUES
    (1, 1, "Alice", "6000"), 
    (2, 1, "Bob", "6100"),
    (3, 2, "Candy", "10000"),
    (4, 2, "Dave", "20000"),
    (5, 3, "Evan","18000"),
    (6, 3, "Freman","1000"),
    (7, 4, "George","1800"),
    (8, 4, "Harry","2000"),
    (9, 5, "Ivan", "15000"),
    (10, 5, "Jim","20000"),
    (11, -1, "Kevin","1500"),
    (12, -1, "Lily","2500");
    ```

2. 查看查询的逻辑执行计划。

    ```SQL
    -- Q1: 查询 emps 的所有列，未查询 depts 中的任何列。
    EXPLAIN LOGICAL SELECT emps.* FROM emps LEFT JOIN depts ON emps.deptno = depts.deptno;
    +-----------------------------------------------------------------------------+
    | Explain String                                                              |
    +-----------------------------------------------------------------------------+
    | - Output => [1:empid, 2:deptno, 3:name, 4:salary]                           |
    |     - SCAN [emps] => [1:empid, 2:deptno, 3:name, 4:salary]                  |
    |             Estimates: {row: 10, cpu: ?, memory: ?, network: ?, cost: 20.0} |
    |             partitionRatio: 1/1, tabletRatio: 10/10                         |
    +-----------------------------------------------------------------------------+
    
    -- Q2: 仅查询 emps 中的 deptno 和 salary，未查询 depts 中的任何列。
    EXPLAIN LOGICAL SELECT emps.deptno, avg(salary) AS mean_salary 
    FROM emps LEFT JOIN depts ON emps.deptno = depts.deptno 
    GROUP BY emps.deptno 
    ORDER BY mean_salary DESC 
    LIMIT 5;
    +-------------------------------------------------------------------------------------------------+
    | Explain String                                                                                  |
    +-------------------------------------------------------------------------------------------------+
    | - Output => [2:deptno, 7:avg]                                                                   |
    |     - TOP-5(FINAL)[7: avg DESC NULLS LAST]                                                      |
    |             Estimates: {row: 3, cpu: ?, memory: ?, network: ?, cost: 138.0}                     |
    |         - TOP-5(PARTIAL)[7: avg DESC NULLS LAST]                                                |
    |                 Estimates: {row: 3, cpu: ?, memory: ?, network: ?, cost: 114.0}                 |
    |             - AGGREGATE(GLOBAL) [2:deptno]                                                      |
    |                     Estimates: {row: 3, cpu: ?, memory: ?, network: ?, cost: 90.0}              |
    |                     7:avg := avg(7:avg)                                                         |
    |                 - EXCHANGE(SHUFFLE) [2]                                                         |
    |                         Estimates: {row: 6, cpu: ?, memory: ?, network: ?, cost: 72.0}          |
    |                     - AGGREGATE(LOCAL) [2:deptno]                                               |
    |                             Estimates: {row: 6, cpu: ?, memory: ?, network: ?, cost: 48.0}      |
    |                             7:avg := avg(4:salary)                                              |
    |                         - SCAN [emps] => [2:deptno, 4:salary]                                   |
    |                                 Estimates: {row: 12, cpu: ?, memory: ?, network: ?, cost: 12.0} |
    |                                 partitionRatio: 1/1, tabletRatio: 10/10                         |
    +-------------------------------------------------------------------------------------------------+
    
    -- Q3: 仅查询 emps 中的列。虽然谓词 name = "R&D" 仅选择 depts 的某些行，但最终结果仅依赖于 emps。
    EXPLAIN LOGICAL SELECT emps.deptno, avg(salary) AS mean_salary
    FROM emps LEFT JOIN
    (SELECT deptno FROM depts WHERE name="R&D") t ON emps.deptno = t.deptno 
    GROUP BY emps.deptno 
    ORDER BY mean_salary DESC
    LIMIT 5;
    +-------------------------------------------------------------------------------------------------+
    | Explain String                                                                                  |
    +-------------------------------------------------------------------------------------------------+
    | - Output => [2:deptno, 7:avg]                                                                   |
    |     - TOP-5(FINAL)[7: avg DESC NULLS LAST]                                                      |
    |             Estimates: {row: 3, cpu: ?, memory: ?, network: ?, cost: 138.0}                     |
    |         - TOP-5(PARTIAL)[7: avg DESC NULLS LAST]                                                |
    |                 Estimates: {row: 3, cpu: ?, memory: ?, network: ?, cost: 114.0}                 |
    |             - AGGREGATE(GLOBAL) [2:deptno]                                                      |
    |                     Estimates: {row: 3, cpu: ?, memory: ?, network: ?, cost: 90.0}              |
    |                     7:avg := avg(7:avg)                                                         |
    |                 - EXCHANGE(SHUFFLE) [2]                                                         |
    |                         Estimates: {row: 6, cpu: ?, memory: ?, network: ?, cost: 72.0}          |
    |                     - AGGREGATE(LOCAL) [2:deptno]                                               |
    |                             Estimates: {row: 6, cpu: ?, memory: ?, network: ?, cost: 48.0}      |
    |                             7:avg := avg(4:salary)                                              |
    |                         - SCAN [emps] => [2:deptno, 4:salary]                                   |
    |                                 Estimates: {row: 12, cpu: ?, memory: ?, network: ?, cost: 12.0} |
    |                                 partitionRatio: 1/1, tabletRatio: 10/10                         |
    +-------------------------------------------------------------------------------------------------+
    
    -- Q4: WHERE 子句中的谓词 depts.name="R&D" 违反了保留基数的条件，因此 depts 不能被裁剪。
    EXPLAIN LOGICAL SELECT emps.deptno, avg(salary) AS mean_salary 
    FROM emps LEFT JOIN depts ON emps.deptno = depts.deptno 
    WHERE depts.name="R&D";
    
    +-----------------------------------------------------------------------------------------------+
    | Explain String                                                                                |
    +-----------------------------------------------------------------------------------------------+
    | - Output => [8:any_value, 7:avg]                                                              |
    |     - AGGREGATE(GLOBAL) []                                                                    |
    |             Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 110.5}                   |
    |             7:avg := avg(7:avg)                                                               |
    |             8:any_value := any_value(8:any_value)                                             |
    |         - EXCHANGE(GATHER)                                                                    |
    |                 Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 105.5}               |
    |             - AGGREGATE(LOCAL) []                                                             |
    |                     Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 101.5}           |
    |                     7:avg := avg(4:salary)                                                    |
    |                     8:any_value := any_value(2:deptno)                                        |
    |                 - HASH/INNER JOIN [2:deptno = 5:deptno] => [2:deptno, 4:salary]               |
    |                         Estimates: {row: 12, cpu: ?, memory: ?, network: ?, cost: 79.5}       |
    |                     - SCAN [emps] => [2:deptno, 4:salary]                                     |
    |                             Estimates: {row: 12, cpu: ?, memory: ?, network: ?, cost: 12.0}   |
    |                             partitionRatio: 1/1, tabletRatio: 10/10                           |
    |                     - EXCHANGE(BROADCAST)                                                     |
    |                             Estimates: {row: 5, cpu: ?, memory: ?, network: ?, cost: 25.0}    |
    |                         - SCAN [depts] => [5:deptno]                                          |
    |                                 Estimates: {row: 5, cpu: ?, memory: ?, network: ?, cost: 5.0} |
    |                                 partitionRatio: 1/1, tabletRatio: 10/10                       |
    |                                 predicate: 6:name = 'R&D'                                     |
    +-----------------------------------------------------------------------------------------------+
    ```

在上述示例中，Q1、Q2 和 Q3 中允许进行表裁剪，而 Q4 违反了保留基数条件，未能执行表裁剪。

### 基于外键的  INNER/LEFT/RIGHT JOIN 表裁剪

**INNER JOIN** 中的表裁剪条件更为严格，因为保留侧必须有外键引用裁剪侧，并且必须确保引用完整性。目前，StarRocks 不强制检查外键约束的一致性。

虽然基于外键的表裁剪更为严格，但也更强大，允许优化器在 INNER JOIN 中利用列等价推断，从而可以在更复杂的场景中使用裁剪。

裁剪的条件如下：

- **被裁剪侧**

  - 被裁剪表必须位于 LEFT JOIN 中的**右侧**或 RIGHT JOIN 中的**左侧**。
  - 在 INNER JOIN 中，被裁剪表必须在连接列上具有唯一键约束，并且保留侧中的每一行必须与裁剪侧中的一行完全匹配。如果保留侧中存在不匹配的行，则无法进行裁剪。

- **连接条件**

  连接必须且只能使用等值条件 (`=`)。保留侧的连接列必须是**外键**，而裁剪侧的连接列必须是**主键**或**唯一键**。这些列必须符合外键约束。

- **输出列**

  - 仅应输出保留侧的列，并且结果应保持与保留侧**相同的基数和重复因子**。
  - 对于 INNER JOIN，可以用保留侧的等价列替换裁剪侧的连接列。如果裁剪侧的所有输出列都是连接列，也可以进行裁剪。

- **NULL****/默认值**

  保留侧的连接列**不能**包含 NULL 或其他不匹配裁剪侧的默认值。

示例：

1. 创建表，定义外键，并插入数据。

    ```SQL
    CREATE TABLE `depts` (
      `deptno` int(11) NOT NULL COMMENT "",
      `name` varchar(25) NOT NULL COMMENT ""
    ) ENGINE=OLAP
    PRIMARY KEY(`deptno`)
    DISTRIBUTED BY HASH(`deptno`) BUCKETS 10;
    
    CREATE TABLE `emps` (
      `empid` int(11) NOT NULL COMMENT "",
      `deptno` int(11) NOT NULL COMMENT "",
      `name` varchar(25) NOT NULL COMMENT "",
      `salary` double NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`empid`)
    DISTRIBUTED BY HASH(`empid`) BUCKETS 10;
    
    ALTER TABLE emps SET ("foreign_key_constraints" = "(deptno) REFERENCES depts(deptno)");
    
    INSERT INTO depts VALUES
    (1, "R&D"),
    (2, "Marketing"), 
    (3, "Community"),
    (4, "DBA"),(5, "POC");
    
    INSERT INTO emps VALUES
    (1, 1, "Alice", "6000"), 
    (2, 1, "Bob", "6100"),
    (3, 2, "Candy", "10000"),
    (4, 2, "Dave", "20000"),
    (5, 3, "Evan","18000"),
    (6, 3, "Freman","1000"),
    (7, 4, "George","1800"),
    (8, 4, "Harry","2000"),
    (9, 5, "Ivan", "15000"),
    (10, 5, "Jim","20000");
    ```

2. 查看查询的逻辑执行计划。

    ```SQL
    -- Q1: 查询 emps 中的 empid 和 name，以及 depts 中的 deptno。
    -- 如连接条件所示，emps.deptno 等价于 depts.deptno，
    -- 因此可以用 emps.deptno 替换 depts.deptno。
    EXPLAIN LOGICAL WITH t0 AS (
    SELECT empid, depts.deptno, emps.name, emps.salary, depts.name AS dept_name 
    FROM emps INNER JOIN depts ON emps.deptno = depts.deptno
    )
    SELECT empid, deptno, name FROM t0;
    +-----------------------------------------------------------------------------+
    | Explain String                                                              |
    +-----------------------------------------------------------------------------+
    | - Output => [7:empid, 8:deptno, 9:name]                                     |
    |     - SCAN [emps] => [7:empid, 8:deptno, 9:name]                            |
    |             Estimates: {row: 12, cpu: ?, memory: ?, network: ?, cost: 18.0} |
    |             partitionRatio: 1/1, tabletRatio: 10/10                         |
    +-----------------------------------------------------------------------------+
    
    -- Q2: 仅查询 emps 中的 salary。
    EXPLAIN LOGICAL SELECT avg(salary) 
    FROM emps INNER JOIN depts ON emps.deptno = depts.deptno;
    +----------------------------------------------------------------------------------------+
    | Explain String                                                                         |
    +----------------------------------------------------------------------------------------+
    | - Output => [7:avg]                                                                    |
    |     - AGGREGATE(GLOBAL) []                                                             |
    |             Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 16.5}             |
    |             7:avg := avg(7:avg)                                                        |
    |         - EXCHANGE(GATHER)                                                             |
    |                 Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 14.0}         |
    |             - AGGREGATE(LOCAL) []                                                      |
    |                     Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 12.0}     |
    |                     7:avg := avg(4:salary)                                             |
    |                 - SCAN [emps] => [4:salary]                                            |
    |                         Estimates: {row: 10, cpu: ?, memory: ?, network: ?, cost: 5.0} |
    |                         partitionRatio: 1/1, tabletRatio: 10/10                        |
    +----------------------------------------------------------------------------------------+
    
    -- Q3: 谓词 name="R&D" 影响最终结果的基数。
    EXPLAIN LOGICAL SELECT emps.deptno, avg(salary) AS mean_salary 
    FROM emps INNER JOIN
    (SELECT deptno FROM depts WHERE name="R&D") t ON emps.deptno = t.deptno 
    GROUP BY emps.deptno
    ORDER BY mean_salary DESC 
    LIMIT 5;
    +-------------------------------------------------------------------------------------------------------+
    | Explain String                                                                                        |
    +-------------------------------------------------------------------------------------------------------+
    | - Output => [2:deptno, 7:avg]                                                                         |
    |     - TOP-5(FINAL)[7: avg DESC NULLS LAST]                                                            |
    |             Estimates: {row: 2, cpu: ?, memory: ?, network: ?, cost: 165.0480769230769}               |
    |         - TOP-5(PARTIAL)[7: avg DESC NULLS LAST]                                                      |
    |                 Estimates: {row: 2, cpu: ?, memory: ?, network: ?, cost: 145.0480769230769}           |
    |             - AGGREGATE(GLOBAL) [2:deptno]                                                            |
    |                     Estimates: {row: 2, cpu: ?, memory: ?, network: ?, cost: 125.04807692307692}      |
    |                     7:avg := avg(4:salary)                                                            |
    |                 - HASH/INNER JOIN [2:deptno = 5:deptno] => [2:deptno, 4:salary]                       |
    |                         Estimates: {row: 10, cpu: ?, memory: ?, network: ?, cost: 100.04807692307692} |
    |                     - EXCHANGE(SHUFFLE) [2]                                                           |
    |                             Estimates: {row: 10, cpu: ?, memory: ?, network: ?, cost: 50.0}           |
    |                         - SCAN [emps] => [2:deptno, 4:salary]                                         |
    |                                 Estimates: {row: 10, cpu: ?, memory: ?, network: ?, cost: 10.0}       |
    |                                 partitionRatio: 1/1, tabletRatio: 10/10                               |
    |                     - EXCHANGE(SHUFFLE) [5]                                                           |
    |                             Estimates: {row: 5, cpu: ?, memory: ?, network: ?, cost: 15.0}            |
    |                         - SCAN [depts] => [5:deptno]                                                  |
    |                                 Estimates: {row: 5, cpu: ?, memory: ?, network: ?, cost: 5.0}         |
    |                                 partitionRatio: 1/1, tabletRatio: 10/10                               |
    |                                 predicate: 6:name = 'R&D'                                             |
    +-------------------------------------------------------------------------------------------------------+
    ```

在上述示例中，Q1 和 Q2 中允许进行表裁剪，而 Q3 由于其谓词影响最终结果的基数，未能执行表裁剪。

