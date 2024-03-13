---
displayed_sidebar: "English"
---

# SHOW META

## Description

Views metadata of CBO statistics, including basic statistics and histograms.

This statement is supported from v2.4.

### View metadata of basic statistics

#### Syntax

```SQL
SHOW STATS META [WHERE]
```

This statement returns the following columns.

| **Column** | **Description**                                              |
| ---------- | ------------------------------------------------------------ |
| Database   | The database name.                                           |
| Table      | The table name.                                              |
| Columns    | The column names.                                            |
| Type       | The type of statistics. `FULL` means full collection and `SAMPLE` means sampled collection. |
| UpdateTime | The latest statistics update time for the current table.     |
| Properties | Custom parameters.                                           |
| Healthy    | The health of statistical information.                       |

### View metadata of histograms

#### Syntax

```SQL
SHOW HISTOGRAM META [WHERE];
```

This statement returns the following columns.

| **Column** | **Description**                                              |
| ---------- | ------------------------------------------------------------ |
| Database   | The database name.                                           |
| Table      | The table name.                                              |
| Column     | The columns.                                                 |
| Type       | Type of statistics. The value is `HISTOGRAM` for histograms. |
| UpdateTime | The latest statistics update time for the current table.     |
| Properties | Custom parameters.                                           |

## References

For more information about collecting statistics for CBO, see [Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md).
