---
displayed_sidebar: docs
sidebar_position: 110
---

# [Preview] Flat JSON

本文介绍 Flat JSON 的基本概念，以及如何使用该功能。

自 2.2.0 版本起，StarRocks 支持 JSON 数据类型，用于支持更加灵活的数据存储。但在查询 JSON 时，大部分场景并不是直接读取完整的 JSON 数据，而是访问指定路径下的数据，举例：

```SQL
-- 将日志中必填的字段存储为固定字段，将其他经常随业务变更的字段打包为 JSON 存储。
SELECT
    time,
    event,
    user,
    get_json_string(remain_json, "$.from_system"),
    get_json_string(remain_json, "$.tag")
FROM logs;
```

由于 JSON 类型的特殊性，在查询中 JSON 类型的性能表现并不如标准类型（INT，STRING 等），其原因有：
- 存储开销：JSON 是半结构化类型，需要存储每行数据的结构信息，导致存储占用多且压缩效率低。
- 查询复杂性：查询时需要根据运行时数据检测数据结构，难以实现向量化执行优化。
- 冗余数据：查询时需读取完整的 JSON 数据，包含大量冗余字段。


StarRocks 引入了 Flat JSON 功能，以提高 JSON 数据查询效率和降低用户使用 JSON 的复杂度。
- 从 3.3.0 版本开始提供此功能，默认情况下关闭，需要手动启用。
- 从 3.4.0 版本开始，默认启用 Flat JSON 功能，无需用户手动操作。

## 什么是 Flat JSON

Flat JSON 的核心原理是在导入时检测 JSON 数据，将 JSON 数据中的公共字段提取为标准类型数据存储。在查询 JSON 时，通过这些公共字段数据优化 JSON 的查询速度。示例数据：

```Plaintext
1, {"a": 1, "b": 21, "c": 3, "d": 4}
2, {"a": 2, "b": 22, "d": 4}
3, {"a": 3, "b": 23, "d": [1, 2, 3, 4]}
4, {"a": 4, "b": 24, "d": null}
5, {"a": 5, "b": 25, "d": null}
6, {"c": 6, "d": 1}
```

在导入上述这组 JSON 数据时，`a` 和 `b` 两个字段在大部分的 JSON 数据中都存在并且其数据类型相似（都是 INT），那么可以将 `a`，`b` 两个字段的数据都从 JSON 中读取出来，单独存储为两列 INT。当查询中使用到这两列时，就可以直接读取 `a`，`b` 两列的数据，无需读取 JSON 中额外的字段，在计算时减少对 JSON 结构的处理开销。


## 验证 Flat JSON 是否生效

导入数据后，可以查询对应列提取的子列：

    ```SQL
    SELECT flat_json_meta(<json_column>)
    FROM <table_name>[_META_];
    ```


可以通过 [Query Profile](../administration/query_profile_overview.md) 验证执行的查询是否受益于 FlatJSON 优化，观测以下几个指标：
  - `PushdownAccessPaths`：下推存储的子字段路径数量。
  - `AccessPathHits`：命中 Flat JSON 子字段的次数，其子项详细打印了具体命中的 JSON。
  - `AccessPathUnhits`：未命中 Flat JSON 子字段的次数，其子项详细打印了具体未命中的 JSON 。
  - `JsonFlattern`：当存在未命中 Flat JSON 时，系统现场提取子列的耗时。

## 使用示例

1. 开启功能（参考其他章节）
2. 创建一张包含 JSON 列的表，本示例使用 INSERT INTO 向表中导入 JSON 数据。

   ```SQL
   CREATE TABLE `t1` (
       `k1` int,
       `k2` JSON,
       `k3` VARCHAR(20),
       `k4` JSON
   )             
   DUPLICATE KEY(`k1`)
   COMMENT "OLAP"
   DISTRIBUTED BY HASH(`k1`) BUCKETS 2
   PROPERTIES ("replication_num" = "3");
      
   INSERT INTO t1 (k1,k2) VALUES
   (11,parse_json('{"str":"test_flat_json","Integer":123456,"Double":3.14158,"Object":{"c":"d"},"arr":[10,20,30],"Bool":false,"null":null}')),
   (15,parse_json('{"str":"test_str0","Integer":11,"Double":3.14,"Object":{"a":"b"},"arr":[1,2,3],"Bool":true,"null":null}')),
   (15,parse_json('{"str":"test_str1","Integer":111,"Double":3.14,"Object":{"a":"b"},"arr":[1,2,3],"Bool":true,"null":null}')),
   (15,parse_json('{"str":"test_str2","Integer":222,"Double":3.14,"Object":{"a":"b"},"arr":[1,2,3],"Bool":true,"null":null}')),
   (15,parse_json('{"str":"test_str2","Integer":222,"Double":3.14,"Object":{"a":"b"},"arr":[1,2,3],"Bool":true,"null":null}')),
   (16,parse_json('{"str":"test_str3","Integer":333,"Double":3.14,"Object":{"a":"b"},"arr":[1,2,3],"Bool":true,"null":null}')),
   (17,parse_json('{"str":"test_str3","Integer":333,"Double":3.14,"Object":{"a":"b"},"arr":[1,2,3],"Bool":true,"null":null}')),
   (18,parse_json('{"str":"test_str5","Integer":444,"Double":3.14,"Object":{"a":"b"},"arr":[1,2,3],"Bool":true,"null":null}')),
   (19,parse_json('{"str":"test_str6","Integer":444,"Double":3.14,"Object":{"a":"b"},"arr":[1,2,3],"Bool":true,"null":null}')),
   (20,parse_json('{"str":"test_str6","Integer":444,"Double":3.14,"Object":{"a":"b"},"arr":[1,2,3],"Bool":true,"null":null}'));
   ```

3. 查看对于 `k2` 列提取的子列。

   ```Plaintext
   SELECT flat_json_meta(k2) FROM t1[_META_];
   +---------------------------------------------------------------------------------------------------------------------------+
   | flat_json_meta(k2)                                                                                                        |
   +---------------------------------------------------------------------------------------------------------------------------+
   | ["nulls(TINYINT)","Integer(BIGINT)","Double(DOUBLE)","str(VARCHAR)","Bool(JSON)","Object(JSON)","arr(JSON)","null(JSON)"] |
   +---------------------------------------------------------------------------------------------------------------------------+
   ```

5. 执行数据查询。

   ```SQL
   SELECT * FROM t1;
   SELECT get_json_string(k2,'\$.Integer') FROM t1 WHERE k2->'str' = 'test_flat_json';
   SELECT get_json_string(k2,'\$.Double') FROM t1 WHERE k2->'Integer' = 123456;
   SELECT get_json_string(k2,'\$.Object') FROM t1 WHERE k2->'Double' = 3.14158;
   SELECT get_json_string(k2,'\$.arr') FROM t1 WHERE k2->'Object' = to_json(map{'c':'d'});
   SELECT get_json_string(k2,'\$.Bool') FROM t1 WHERE k2->'arr' = '[10,20,30]';
   ```

7. 查看 [Query Profile](../administration/query_profile_overview.md) 中 Flat JSON 相关指标
   ```yaml
      PushdownAccessPaths: 2
      - Table: t1
      - AccessPathHits: 2
      - __MAX_OF_AccessPathHits: 1
      - __MIN_OF_AccessPathHits: 1
      - /k2: 2
         - __MAX_OF_/k2: 1
         - __MIN_OF_/k2: 1
      - AccessPathUnhits: 0
      - JsonFlattern: 0ns
   ```
   
## 功能限制

- StarRocks 所有表类型都支持 Flat JSON。
- 兼容历史数据，无须重新导入。历史数据会和 Flat JSON 打平的数据共存。
- 历史数据在不发生变更时不会自动应用 Flat JSON 优化，但导入新数据，或者发生 Compactoin，会应用 Flat JSON 优化
- 开启 Flat JSON 后会增加导入 JSON 的耗时，提取的 JSON 越多，耗时越长。
- Flat JSON 仅能支持物化 JSON Object 中的公共 Key，不支持物化 JSON Array 中的 Key
- Flat JSON 并不改变数据的排序方式，因此查询性能、数据压缩率仍然会受到数据排序的影响，为了得到最优性能，可以进一步调整数据的排序方式 


## 版本说明

StarRocks 存算一体集群自 v3.3.0 起支持 Flat JSON，存算分离集群自 v3.3.3 起支持。

在 v3.3.0、v3.3.1、v3.3.2 版本中：
- 导入数据时，支持提取公共字段、单独存储为 JSON 类型，未实现类型推导。
- 会同时存储提取的列和原始 JSON 数据。提取的数据会在原始数据删除时一起删除。

自 v3.3.3 版本起：
- Flat JSON 提取的结果分为公共的列和保留字段列，当所有 JSON Schema 一致时，不会生成保留字段列。
- Flat JSON 仅存储公共字段列和保留字段列，不会再额外存储原始 JSON 数据。
- 导入数据时，公共字段会自动推导类型为 BIGINT/LARGEINT/DOUBLE/STRING,不能识别的类型推导为 JSON 类型，保留字段列会存储为 JSON 类型。

## 开启 Flat JSON 功能（3.4 之前版本）

1. 修改 BE 配置: `enable_json_flat`， 3.4 之前版本默认为 `false`。修改方式参考
[Configure BE parameters](../administration/management/BE_configuration.md#configure-be-parameters)
2. 开启 FE 裁剪功能：`SET GLOBAL cbo_prune_json_subfield = true;`

## 其他可选 BE 配置

- [json_flat_null_factor](../administration/management/BE_configuration.md#json_flat_null_factor)
- [json_flat_column_max](../administration/management/BE_configuration.md#json_flat_column_max)
- [json_flat_sparsity_factor](../administration/management/BE_configuration.md#json_flat_sparsity_factor)
- [enable_compaction_flat_json](../administration/management/BE_configuration.md#enable_compaction_flat_json)
- [enable_lazy_dynamic_flat_json](../administration/management/BE_configuration.md#enable_lazy_dynamic_flat_json)
