---
displayed_sidebar: docs
sidebar_position: 130
---

# 表达式 JIT 编译

本文描述了如何在 StarRocks 中启用和配置表达式的 JIT 编译功能。

## 概述

即时编译（Just-in-time，JIT）是指在运行时生成机器代码并执行。与解释器相比，JIT 编译器可以显著提高部分常用代码的执行效率。StarRocks 支持对某些复杂表达式进行 JIT 编译，从而带来大幅度性能提升。

## 使用方法

从 v3.3.0 开始，StarRocks 默认对内存上限（通过 BE 配置项 `mem_limit` 进行配置）大于或等于 16 GB 的 BE 节点启用 JIT 编译。由于 JIT 编译会消耗一定的内存资源，因此对于内存小于 16 GB 的 BE 节点，JIT 编译默认禁用。

您可以使用以下参数启用和配置表达式 JIT 编译功能。

### jit_lru_cache_size (BE 配置)

- 默认值：0
- 类型：Int
- 单位：GB
- 是否动态：是
- 描述：JIT 编译的 LRU 缓存大小。如果设置为大于 0，则表示实际的缓存大小。如果设置为小于或等于 0，系统将自适应设置缓存大小，使用的公式为 `jit_lru_cache_size = min(mem_limit*0.01, 1GB)` （节点的 `mem_limit` 必须大于或等于 16 GB）。
- 引入版本：-

### jit_level (系统变量)

- 描述：表达式 JIT 编译的启用级别。有效值：
  - `1`：系统为可编译表达式自适应启用 JIT 编译。
  - `-1`：对所有可编译的非常量表达式启用 JIT 编译。
  - `0`：禁用 JIT 编译。如果该功能返回任何错误，您可以手动禁用。
- 默认值：1
- 数据类型：Int
- 引入版本：-

## 功能支持

### 支持的表达式

- `+`、`-`、`*`、`/`、`%`、`&`、`|`、`^`、`>>`、`<<`
- 通过 CAST 进行的数据类型转换
- CASE WHEN
- `=`、`!=`、`>`、`=`、`<`、`<=`、`<=>`
- `AND`、`OR`、`NOT`

### 支持的操作符

- 用于过滤的 OLAP Scan Operator
- Projection Operator
- 用于表达式的 Aggregate Operator
- HAVING
- 用于表达式的 Sort Operator

### 支持的数据类型

- BOOLEAN
- TINYINT
- SMALLINT
- INT
- BIGINT
- LARGEINT
- FLOAT
- DOUBLE

