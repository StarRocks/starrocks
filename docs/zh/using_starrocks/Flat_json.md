# Flat JSON

本文介绍 Flat JSON 的基础概念，以及如何使用该功能。

自 2.2.0 版本起，StarRocks 支持 JSON 数据类型，用于支持更加灵活的数据存储。使用 JSON 类型大多数场景是因为需要存储的数据结构易变，难以维护结构变更。但在查询 JSON 时，大部分场景并不是直接读取完整的 JSON 数据，而是访问指定路径下的数据，举例：

```sql
// 将日志中必填的字段存储为固定字段，将其他经常随业务变更的字段打包为 JSON 存储。
select time, event, user, get_json_string(remain_json, "$.from_system"), get_json_string(remain_json, "$.tag")
from logs;
```

但由于 JSON 类型的特殊性，在查询中 JSON 类型的性能表现并不如标准类型（INT，STRING 等），其原因有：

- JSON 类型是半结构化类型，在存储上需要存储每行数据的结构信息，存储占用多，压缩效率低。
- 在查询时，需要根据运行时数据来检测数据结构，难以实现向量化执行优化。
- 在查询时，需要读取完整的 JSON 数据，其中包含了大量冗余字段。

StarRocks 从 3.3.0  版本起，支持 Flat JSON 功能，旨在优化 JSON 的查询性能。

## 什么是 Flat JSON

Flat JSON 的核心原理是在导入时检测 JSON 数据，将 JSON 数据中的公共字段提取为标准类型数据存储。在查询 JSON 时，通过这些公共字段数据优化 JSON 的查询速度，例如：

```plain text
1, {"a": 1, "b": 21, "c": 3, "d": 4}
2, {"a": 2, "b": 22, "d": 4}
3, {"a": 3, "b": 23, "d": [1, 2, 3, 4]}
4, {"a": 4, "b": 24, "d": null}
5, {"a": 5, "b": 25, "d": null}
6, {"c": 6, "d": 1}
```

在导入上述这样一组 JSON 数据时，`a` 和 `b` 两个字段在大部分的 JSON 数据中都存在并且其数据类型相似（都是 INT），那么可以将 `a`，`b` 两个字段的数据都从 JSON 中读取出来，单独存储为两列 INT。当查询中使用到这两列时，就可以直接读取 `a`，`b` 两列的数据，减少查询时读取 JSON 中额外的字段数据，在计算时减少对 JSON 结构的处理开销。

## 使用 Flat Json

StarRocks 当前对 Flat JSON 的支持情况：

* 导入时，支持提取公共字段，自动推导公共字段类型。
* 目前只支持最顶层 JSON 字段的提取。
* 目前会同时存储提取列和原始 JSON 数据。
* 兼容历史数据，无须重新导入。
* 向历史表导入新数据时，自动通过 Compaction 完成 Flat JSON 操作。

使用 Flat JSON 需要在 BE 上做如下配置并重启 BE 生效。开启后新导入的 JSON 数据会自动打平。

```bash
curl -XPOST http://be_host:http_port/api/update_config?enable_json_flat=true
```

在查询 SQL 时，设置 Session 变量：

```sql
set cbo_prune_json_subfield = true;
```

无须其他配置，既可开始使用。

## 验证 Flat JSON 是否生效

导入后，使用SQL可以查询对应列提取的子列：
```sql
select flat_json_meta(json_column), count(1) from tableA[_META];
```

查询中，可以通过 [Query Profile](../administration/query_profile.md) 观察其中几个相关指标：

* `PushdownAccessPaths`：下推存储的子字段路径数量。
* `AccessPathHits`：命中 Flat JSON 优化的存储文件数量，其子项详细打印了具体命中的 JSON。 
* `AccessPathUnhits`：未命中 Flat JSON 优化的存储文件数据量，其子项详细打印了具体未命中的 JSON 。
* `JsonFlattern`：当存在未命中 Flat JSON 优化时，执行动态 Flat JSON 的耗时。

## 其他配置

BE 配置：

* `enable_json_flat`：控制 Flat JSON 功能是否开启，默认为 `ture`。
* `json_flat_null_factor`：控制 Flat JSON 时，提取列的 NULL 值占比阈值，高于该比例不进行提取，默认为 0.3。
* `json_flat_internal_column_min_limit`：控制 Flat JSON 时，JSON 内部字段数量限制，低于该数量的 JSON 不执行 Flat JSON 优化，默认为 5。
* `json_flat_column_max`：控制 Flat JSON 时，最多提取的子列数量，默认为 20。
* `json_flat_sparsity_factor`：控制 Flat JSON 时，同名列的占比阈值，当同名列占比低于该值时不进行提取，默认为 0.9。

## 注意事项

* 开启 Flat JSON 后，提取列会占用额外的存储资源。
* 开启 Flat JSON 后，会加大导入 JSON 的耗时，提取的 JSON 越多，耗时越长。
* 开启 Flat JSON 后，Compaction 的耗时和内存使用量会增高。
* 系统变量 `cbo_prune_json_subfield` 只有在命中 Flat JSON 时才有效果，其他情况下可能存在性能负优化。
* 如果遇到 Crash 或者查询报错，可以通过关闭 BE 参数 `enable_json_flat` 以及 Session 变量 `cbo_prune_json_subfield` 进行规避。

## 关键字

FLAT JSON, JSON, VARIANT, FLAT
