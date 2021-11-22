# CBO Optimizer

## Background

In version 1.16.0, StarRocks introduced a new optimizer to generate better execution plans for complex Ad-hoc scenarios. Leveraging the cascades framework, StarRocks is able to implement the Cost-based Optimizer (CBO) into the query planning framework. This optimizer provides more statistical information for cost estimation, and adds new rules for  query transformation and implementation, allowing StarRocks to quickly find the optimal plan among a large number  of query planning choices.

## Instructions

### Enable automatic sampling collection of statistical information

Before enabling the new optimizer, you need to enable statistical information collection by modifying the FE Config.

~~~Apache
enable_statistic_collect = true
~~~

Then restart the FE.

### Query to enable the new optimizer

> Note: Before enabling the new optimizer, it is recommended to turn on automatic sampling of statistical information collection for 1 ~ 2 days.

Enable global granularity:

~~~SQL
set global enable_cbo = true;
~~~

Enable session granularity:

~~~SQL
set enable_cbo = true;

~~~

Enable single SQL granularity:

~~~SQL
SELECT /*+ SET_VAR(enable_cbo = true) */ * from table;
~~~
