---
displayed_sidebar: docs
sidebar_position: 120
---

# Accelerate Cardinality-Preserving Joins

This topic describes how to accelerate cardinality-preserving joins with table pruning. This feature is supported from v3.1 onwards.

## Overview

A cardinality-preserving join ensures that the cardinality and duplication factor of the output rows remain the same as those of one of the input tables in the join. Consider the following examples:

- **Inner Join:**

  ```SQL
  SELECT A.* FROM A INNER JOIN B ON A.fk = B.pk;
  ```

  In this case, `A.fk` (Foreign Key) is **NOT NULL** and references `B.pk` (Primary Key). Each row in `A` matches exactly one row in `B`, so the cardinality and duplication factor of the output match those of `A`.

- **Left Join:**

  ```SQL
  SELECT A.* FROM A LEFT JOIN B ON A.fk = B.pk;
  ```

  Here, `A.fk` references `B.pk`, but `A.fk` can contain NULL values. Each row in `A` matches at most one row in `B`. As a result, the cardinality and duplication factor of the output remain aligned with `A`.

<br />

In these types of joins, if the final output columns only depend on columns from table `A`, and the columns from table `B` are not used, table `B` can be pruned from the join. Since v3.1, StarRocks supports **table pruning** in cardinality-preserving joins, which can occur in Common Table Expressions (CTEs), logical views, and subqueries.

### Use case: Real-time feature selection in risk control

The table pruning feature for cardinality-preserving join is particularly useful in scenarios like **real-time feature selection** for risk control. In this context, users need to select data from a large number of tables, often dealing with a combinatorial explosion of columns and tables. The following characteristics are common in the risk control domain:

- Numerous features are spread across many independently updated tables.
- Fresh data must be visible and queryable in real time.
- A **flat logical view** is used to simplify the data model, making SQL for column extraction more succinct and productive.

Using a flat logical view, rather than other accelerated data layers, helps users access real-time data efficiently. In each column extraction query, only a few tables (not all tables in the logical view) are required to be joined. By pruning the unused tables from these queries, you can reduce the number of joins and improve performance.

### Feature support

The table pruning feature supports multi-table joins in both **star schema** and **snowflake schema**. Multi-table joins can appear in CTEs, logical views, and subqueries, allowing for more efficient query execution.

Currently, the table pruning feature is only supported on OLAP tables and Cloud-native tables. External tables in multi-join can not be pruned.

## Usage

The following examples use the TPC-H dataset.

### Prerequisites

To use the table pruning feature, the following conditions must be met:

1. Enable table pruning
2. Set key constraints

#### Enable table pruning

By default, table pruning is disabled. You need to enable the feature by configuring the following session variables:

```SQL
-- Enable RBO-phase table pruning.
SET enable_rbo_table_prune=true;
-- Enable CBO-phase table pruning.
SET enable_cbo_table_prune=true; 
-- Enable RBO-phase table pruning for the UPDATE statement on the Primary Key tables.
SET enable_table_prune_on_update = true;
```

#### Set key constraints

Tables to be pruned must have Unique Key or Primary Key constraints at least in LEFT or RIGHT Joins. To prune tables in INNER JOIN, you must define Foreign Key constraints in addition to Unique Key or Primary Key constraints.

Primary Key tables and Unique Key tables have their implicit Primary Key or Unique Key constraints naturally built into them. However, for Duplicate Key tables, you must manually define Unique Key constraints, and ensure that no duplicate rows exist. Note that StarRocks does not enforce Unique Key constraints on Duplicate Key tables. Instead, it treats them as optimization hints for more aggressive query planning.

Example:

```SQL
-- Define the Unique Key constraint during table creation.
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

-- Or you can define the Unique Key constraint after table creation.
ALTER TABLE lineitem SET ("unique_constraints" = "l_orderkey,l_linenumber");
```

Foreign Key constraints, on the other hand, must be defined explicitly. Similar to Unique Key constraints on Duplicate Key tables, Foreign Key constraints act as hints for the optimizer. StarRocks does not enforce Foreign Key constraint consistency. You must ensure data integrity when producing and ingesting data into StarRocks.

Example:

```SQL
-- Create the table to be referenced in the Foreign Key constraint.
-- Note the column to be referenced must have Unique Key or Primary Key constraints.
-- In this example, `p_partkey` is the Primary Key of the table `part`.
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

-- Define the Foreign Key constraint during table creation.
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

-- Or you can define the Foreign Key constraint after table creation.
ALTER TABLE lineitem SET ("foreign_key_constraints" =  "(l_partkey) REFERENCES part(p_partkey)");
```

### Table pruning in LEFT/RIGHT JOINs based on Unique or Primary Keys

Table pruning in LEFT or RIGHT JOINs does not require that the retained side of the join has a Foreign Key referencing the pruned side. This makes pruning more flexible and robust, even if reference integrity cannot be guaranteed.

Pruning in LEFT/RIGHT JOINs based on Unique or Primary keys has **less strict requirements** compared to INNER JOIN pruning based on Foreign Keys.

The conditions for pruning are:

- **Pruned Side**

  The pruned table must be the **right side** in a LEFT JOIN or the **left side** in a RIGHT JOIN.

- **Join Conditions**

  The join must use equality conditions (`=`) only, and the joined columns of the pruned side must be a **superset** of the Unique or Primary keys.

- **Output Columns**

  Only the columns from the retained side should be output, and the result should maintain the **same cardinality and duplication factor** as the retained side.

- **NULL/Default Values**

  The joined columns in the retained side **can** contain NULL or other default values that do not match the pruned side.

Example:

1. Create tables and insert data.

    ```SQL
    -- The table `depts` has its Primary Key constraint on the column `deptno`.
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

2. View the logical execution plans of the queries.

    ```SQL
    -- Q1: Query all columns of `emps` columns. Not all columns of `depts` are queried.
    EXPLAIN LOGICAL SELECT emps.* FROM emps LEFT JOIN depts ON emps.deptno = depts.deptno;
    +-----------------------------------------------------------------------------+
    | Explain String                                                              |
    +-----------------------------------------------------------------------------+
    | - Output => [1:empid, 2:deptno, 3:name, 4:salary]                           |
    |     - SCAN [emps] => [1:empid, 2:deptno, 3:name, 4:salary]                  |
    |             Estimates: {row: 10, cpu: ?, memory: ?, network: ?, cost: 20.0} |
    |             partitionRatio: 1/1, tabletRatio: 10/10                         |
    +-----------------------------------------------------------------------------+
    
    -- Q2: Query only `deptno` and `salary` in `emps`. No column in `depts` is queried.
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
    
    -- Q3: Only columns in `emps` are queried. Although predicate `name = "R&D"` only 
    -- selects certain rows of `depts`, the final results only depends on `emps`.
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
    
    -- Q4: The predicate `depts.name="R&D"` in the WHERE clause breaches the 
    -- cardinality preserving conditions, so `depts` can not be pruned.
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

In the above example, table pruning is allowed in Q1, Q2, and Q3 as shown in the execution plan, while Q4, which breaches the cardinality preserving conditions, fails to perform table pruning.

### Table Pruning in INNER/LEFT/RIGHT JOINs Based on Foreign Keys

Table pruning in **INNER JOINs** is more restrictive because the retained side must have a **Foreign Key** that references the pruned side, and reference integrity must be ensured. Currently, StarRocks does not enforce consistency checks on Foreign Key constraints. 

Although table pruning based on Foreign Keys is more strict, it is also more powerful. It allows the optimizer to leverage column equivalence inference in INNER JOINs, making pruning possible in more complex scenarios.

The conditions for pruning are as follows:

- **Pruned Side**

  - In a LEFT JOIN, the pruned table must be on the right side. In a RIGHT JOIN, it must be on the left side.
  - In an INNER JOIN, the pruned table must have a Unique Key constraint on the joined columns, and each row in the retained side must match exactly one row in the pruned side. If unmatched rows exist in the retained side, pruning cannot occur.

- **Join Conditions**

  The join must use equality conditions (`=`) only. The joined columns in the retained side must be **Foreign Keys**, while the joined columns in the pruned side must be **Primary Keys** or **Unique Keys**. These columns must align according to the Foreign Key constraints.

- **Output Columns**

  - Only the columns from the retained side should be output, and the result should maintain the **same cardinality and duplication factor** as the retained side.
  - For INNER JOINs, joined columns from the pruned side can be substituted by their equivalent columns in the retained side. If all output columns from the pruned side are joined columns, pruning can also occur.

- **NULL/Default Values:**

  Joined columns in the retained side **cannot** contain NULL or other default values that do not match the pruned side.

Example:

1. Create tables, define the Foreign Key, and insert data.

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

2. View the logical execution plans of the queries.

    ```SQL
    -- Q1: Query `empid` and `name` in `emps`, and `deptno` in `depts`.
    -- However, as shown in the Join condition, `emps.deptno` is equivalent to `depts.deptno`,
    -- so `emps.deptno` can substitute `depts.deptno`.
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
    
    -- Q2: Only query `salary` in `emps`.
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
    
    -- Q3: The predicate `name="R&D"` affects the cardinality of final results.
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

In the above example, table pruning is allowed in Q1, and Q2 as shown in the execution plan, while Q3, whose predicate affects the cardinality of final results, fails to perform table pruning.

