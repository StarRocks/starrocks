---
displayed_sidebar: docs
sidebar_position: 130
---

# JIT Compilation for Expressions

This topic describes how to enable and configure JIT compilation for expressions in StarRocks.

## Overview

Just-in-time compilation (JIT) is to generate the machine code and execute it in runtime. Compared to Interpreter, JIT compiler can significantly improve the execution efficiency of the most used code parts. StarRocks supports JIT compilation for certain complex expressions, allowing a multiplied performance improvement.

## Usage

As of v3.3.0, StarRocks enables JIT compilation by default for BE nodes with a memory limit (configured by BE configuration item `mem_limit`) greater or equal to 16 GB. Because JIT compilation will consume a certain amount of memory resources, it is disabled by default for BE nodes with less than 16 GB of memory.

You can enable and configure JIT compilation for expressions using the following parameters.

### jit_lru_cache_size (BE configuration)

- Default: 0
- Type: Int
- Unit: GB
- Is mutable: Yes
- Description: The LRU cache size for JIT compilation. It represents the actual size of the cache if it is set to greater than 0. If it is set to less than or equal to 0, the system will adaptively set the cache using the formula `jit_lru_cache_size = min(mem_limit*0.01, 1GB)` (while `mem_limit` of the node must be greater or equal to 16 GB).
- Introduced in: -

### jit_level  (System variable)

- Description: The level at which JIT compilation for expressions is enabled. Valid values:
  - `1`: The system adaptively enables JIT compilation for compilable expressions.
  - `-1`: JIT compilation is enabled for all compilable, non-constant expressions.
  - `0`: JIT compilation is disabled. You can disable it manually if any error is returned for this feature.
- Default: 1
- Data type: Int
- Introduced in: -

## Feature support

### Supported expressions

- `+`, `-`, `*`, `/`,  `%`, `&`, `|`, `^`, `>>`, `<<`
- Data type conversion with CAST
- CASE WHEN
- `=`, `!=`, `>`, `>=`, `<`, `<=`, `<=>`
- `AND`, `OR`, `NOT`

### Supported operators

- OLAP Scan Operator for filter
- Projection Operator
- Aggregate Operator for expression
- HAVING
- Sort Operator for expression

### Supported data types

- BOOLEAN
- TINYINT
- SMALLINT
- INT
- BIGINT
- LARGEINT
- FLOAT
- DOUBLE

