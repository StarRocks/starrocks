# Database Introspection Design

This document outlines the design of the database introspection (or reflection) process in the `sqlalchemy-starrocks` dialect. Introspection is the process of examining a live database to determine the schema of its tables, views, and other objects.

## Strategy

### Getting Schema Information

The introspection process employs a multi-tiered strategy to gather schema information, prioritizing structured sources and falling back to less structured ones when necessary.

#### 1. Primary Source: `information_schema`

The primary and preferred source for schema information is the ANSI-standard `information_schema`. (StarRocks will also use some dialect views in `information_schema`.)
The dialect queries the following views to gather the bulk of the information:

- `information_schema.tables`
- `information_schema.columns`
- `information_schema.views`
- `information_schema.materialized_views`
- `information_schema.table_constraints`
- `information_schema.key_column_usage`
- `information_schema.tables_config`: StarRocks specific.

Using `information_schema` is fast, efficient, and provides structured data that is easy to parse.

#### 2. Fallback Sources: `SHOW` Commands

For information that is not available in `information_schema` (such as StarRocks-specific properties like `PARTITION BY`, `DISTRIBUTED BY`, `ORDER BY`), the dialect falls back to using `SHOW` commands:

- **`SHOW CREATE TABLE <table>`**: This command provides the full DDL for a table, which contains all the StarRocks-specific clauses.
- **`SHOW FULL COLUMNS FROM <table>`**: This is used to retrieve column-level details, including key columns and aggregate types for `AGGREGATE KEY` tables.

### Reflecting Column Aggregate Types

For `AGGREGATE KEY` tables, the aggregate type of each column is not available in `information_schema`. The dialect discovers this information by parsing the `Type` field from the output of `SHOW FULL COLUMNS FROM <table>`.

This parsed aggregate type is then passed into the constructor for the reflected `Column` object as a `starrocks_AGG_TYPE` keyword argument. Because `Column` is also a `DialectKWArgs` object (like a `Table`), SQLAlchemy automatically normalizes this into the `column.dialect_options['starrocks']` dictionary, making it available for the `compare` process.

### Parsing DDL into Structured Objects

A key feature of the introspection process is that it does not treat all DDL clauses as opaque strings. For complex attributes like `PARTITION BY` and `DISTRIBUTED BY`, the dialect parses the string output from `SHOW CREATE TABLE` into structured Python objects:

- **`ReflectionPartitionInfo`**: Represents the partitioning strategy.
- **`ReflectionDistributionInfo`**: Represents the distribution (including bucket number) strategy.

These objects hold the parsed details of the clause. They also implement `__str__` and `__repr__` methods, which are crucial for the comparison and rendering stages. These methods can normalize the representation of the clause, ensuring that semantically identical but textually different clauses can be compared accurately.

> sqlacodegen will render these options by using `{one_property!r}` to get the value.

## Current Limitations and Future Improvements

The primary limitation of the current approach is the reliance on string matching and regular expressions to parse the output of `SHOW CREATE TABLE`.

### The Challenge with String Matching

Parsing DDL with regular expressions is inherently brittle. While the current implementation covers many common cases, it may fail to correctly parse complex or unusually formatted `PARTITION BY` or `DISTRIBUTED BY` clauses. This can lead to incorrect schema reflection and, consequently, flawed autogeneration in Alembic.

> Especially, the partition definition and distribution definitation will vary time to time. This tool can't handle them accurately and exactly.

### Future Improvement: ANTLR Parser

To address this limitation, a more robust solution would be to introduce a proper SQL parser. A potential candidate is **ANTLR (ANother Tool for Language Recognition)**, which can be used to build a full-featured parser from a StarRocks SQL grammar.

By using an ANTLR-based parser, the dialect could:

- **Accurately Parse DDL**: Precisely extract some clauses and properties from the `SHOW CREATE TABLE` output without the risk of regex failures on edge cases.
- **Improve Introspection Reliability**: Ensure that the reflected schema is always a correct representation of the database state.
- **Enhance Autogeneration**: Provide a more reliable foundation for Alembic's `compare` process, leading to more accurate schema change detection. Because, the `SHOW CREATE TABLE` is always more accurate and in-time.

The introduction of an ANTLR parser would be a significant step forward in making the dialect more robust and reliable, but also much complicate.
