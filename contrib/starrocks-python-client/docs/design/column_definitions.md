# StarRocks Column Definitions

This document provides a detailed comparison between the column definition syntax specified in the StarRocks grammar (`StarRocks.g4`) and the support provided by the `starrocks-python-client` (SQLAlchemy dialect), with notes on how it relates to the baseline MySQL dialect support.

## StarRocks Column Definition vs. MySQL Dialect

| Feature/Clause       | `StarRocks.g4` Rule    | MySQL Dialect Support (SQLAlchemy) | `starrocks-python-client` Status                                        |
| :------------------- | :--------------------- | :--------------------------------- | :---------------------------------------------------------------------- |
| **Column Name**      | `identifier`           | Yes                                | **Supported**                                                           |
| **Data Type**        | `type?`                | Yes (for standard types)           | **Supported** (including StarRocks-specific types like `BITMAP`, `HLL`) |
| **Character Set**    | `charsetName?`         | Yes                                | Not explicitly supported (rarely used in StarRocks)                     |
| **Key Column**       | `KEY?`                 | No (handled at table level)        | **Supported** (via `info={'starrocks_is_agg_key': True}`)               |
| **Aggregate Type**   | `aggDesc?`             | No                                 | **Supported** (via `info={'starrocks_agg': '...'}`)                     |
| **Nullability**      | `columnNullable?`      | Yes                                | **Supported** (`nullable=True/False`)                                   |
| **Default Value**    | `defaultDesc?`         | Yes                                | **Supported** (`server_default=...`)                                    |
| **Auto Increment**   | `AUTO_INCREMENT`       | Yes                                | **Supported** (`autoincrement=True`)                                    |
| **Generated Column** | `generatedColumnDesc?` | Yes (`Computed`)                   | **Supported**                                                           |
| **Comment**          | `comment?`             | Yes                                | **Supported** (`comment=...`)                                           |

## Summary of Progress and Gaps

- **Well-Supported Features**: We have solid support for most of the core column definition features, including data types (both standard and StarRocks-specific), nullability, default values, auto-increment, generated columns, and comments. We also have mechanisms to handle StarRocks' aggregate key and aggregate type specifications.

- **`KEY` Keyword**: StarRocks allows a `KEY` keyword directly in a column definition, which is syntactic sugar to indicate that the column is part of the key in an `AGGREGATE KEY` table. While our dialect supports this concept, it's done through the `info` dictionary on the `Column` object rather than a direct keyword in the column definition. This is a reasonable and idiomatic way to handle it in SQLAlchemy.

- **`charsetName`**: The `CHARSET` clause is rarely used in StarRocks, as character sets are typically managed at the database or table level. We haven't implemented specific support for this in the dialect, and it's a low-priority item.

- **`aggDesc` (Aggregate Type)**: Our dialect supports specifying the aggregate type for columns in `AGGREGATE KEY` tables using the `info` dictionary, which aligns well with how SQLAlchemy handles dialect-specific options.

Overall, the dialect is in good shape regarding column definitions. We've covered the most critical and frequently used features, and the StarRocks-specific aspects are handled in a way that is consistent with SQLAlchemy's extension mechanisms.
