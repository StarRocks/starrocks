---
displayed_sidebar: docs
sidebar_position: 110
---

import Beta from '../_assets/commonMarkdown/_beta.mdx'

# Flat JSON

<Beta />

本文介绍了Flat JSON的基本概念及如何使用此功能。

从2.2.0版本开始，StarRocks支持JSON数据类型，以实现更灵活的数据存储。然而，在查询JSON时，大多数场景并不涉及直接读取整个JSON数据，而是访问指定路径的数据。例如：

```SQL
-- 将日志中需要的字段存储为固定字段，将其他随业务频繁变化的字段打包为JSON。
SELECT
    time,
    event,
    user,
    get_json_string(remain_json, "$.from_system"),
    get_json_string(remain_json, "$.tag")
FROM logs;
```

由于JSON类型的特殊性，其查询性能不如标准类型（如INT、STRING等）。原因包括：
- 存储开销：JSON是一种半结构化类型，需要存储每行的结构信息，导致存储使用量高且压缩效率低。
- 查询复杂性：查询需要基于运行时数据检测数据结构，难以实现向量化执行优化。
- 冗余数据：查询需要读取整个JSON数据，其中包含许多冗余字段。

StarRocks引入了Flat JSON功能，以提高JSON数据查询效率并降低使用JSON的复杂性。
- 此功能从3.3.0版本开始提供，默认禁用，需要手动启用。

## 什么是Flat JSON

Flat JSON的核心原理是在导入时检测JSON数据，并从JSON数据中提取常用字段，作为标准类型数据存储。在查询JSON时，这些常用字段优化了JSON的查询速度。示例数据：

```Plaintext
1, {"a": 1, "b": 21, "c": 3, "d": 4}
2, {"a": 2, "b": 22, "d": 4}
3, {"a": 3, "b": 23, "d": [1, 2, 3, 4]}
4, {"a": 4, "b": 24, "d": null}
5, {"a": 5, "b": 25, "d": null}
6, {"c": 6, "d": 1}
```

在导入上述JSON数据时，字段`a`和`b`在大多数JSON数据中存在且数据类型相似（均为INT）。因此，可以从JSON中提取字段`a`和`b`的数据，并分别存储为两个INT列。当在查询中使用这两列时，可以直接读取其数据，而无需处理额外的JSON字段，从而减少处理JSON结构的计算开销。

## 启用 Flat JSON

从 v3.4 版本起，Flat JSON 默认全局启用。对于 v3.4 之前的版本，必须手动启用此功能。

从 v4.0 版本起，此功能可在表级别进行配置。

### 为 v3.4 前版本启用

1. 修改 BE 配置：`enable_json_flat`，在 v3.4 版本之前默认为 `false`。修改方法参考 [配置 BE 参数](../administration/management/BE_configuration.md#configure-be-parameters)。
2. 启用FE分区裁剪功能：

   ```SQL
   SET GLOBAL cbo_prune_json_subfield = true;
   ```

### 在表级别启用 Flat JSON 功能

在表级别设置与 Flat JSON 相关的属性自 v4.0 起支持。

1. 在创建表时，您可以设置 `flat_json.enable` 及其他与 Flat JSON 相关的属性。如需详细说明，请参阅 [CREATE TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#在表级别设置-flat-json-属性)。

   或者，您可以使用 [ALTER TABLE](../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) 语句设置这些属性。

   示例：

   ```SQL
   ALTER TABLE t1 SET ("flat_json.enable" = "true");
   ALTER TABLE t1 SET ("flat_json.null.factor" = "0.1");
   ALTER TABLE t1 SET ("flat_json.sparsity.factor" = "0.8");
   ALTER TABLE t1 SET ("flat_json.column.max" = "90");
   ```

2. 启用FE分区裁剪功能：

   ```SQL
   SET GLOBAL cbo_prune_json_subfield = true;
   ```

## 验证Flat JSON是否有效

导入数据后，可以查询相应列的提取子列：

```SQL
SELECT flat_json_meta(<json_column>)
FROM <table_name>[_META_];
```

您可以通过观察以下指标，在[Query Profile](../best_practices/query_tuning/query_profile_overview.md)中验证执行的查询是否受益于Flat JSON优化：
- `PushdownAccessPaths`: 推送到存储的子字段路径数量。
- `AccessPathHits`: Flat JSON子字段命中次数，包含具体JSON命中信息。
- `AccessPathUnhits`: Flat JSON子字段未命中次数，包含具体JSON未命中信息。
- `JsonFlattern`: 当Flat JSON未命中时，现场提取子列所花费的时间。

## 使用示例

1. 启用该功能（参考其他章节）
2. 创建包含JSON列的表。在此示例中，使用INSERT INTO将JSON数据加载到表中。

   ```SQL
    -- 方法1：创建包含JSON列的表，并在创建时配置Flat JSON。仅支持存算一体集群。
   CREATE TABLE `t1` (
       `k1` int,
       `k2` JSON,
       `k3` VARCHAR(20),
       `k4` JSON
   )             
   DUPLICATE KEY(`k1`)
   COMMENT "OLAP"
   DISTRIBUTED BY HASH(`k1`) BUCKETS 2
   PROPERTIES (
     "replication_num" = "3",
     "flat_json.enable" = "true",
     "flat_json.null.factor" = "0.5",
     "flat_json.sparsity.factor" = "0.5",
     "flat_json.column.max" = "50");
   )
   
   -- 方法2：需要启用Flat JSON功能，此方法适用于存算一体和存算分离集群。
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

3. 查看`k2`列的提取子列。

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

7. 在[Query Profile](../best_practices/query_tuning/query_profile_overview.md)中查看Flat JSON相关指标
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

## 相关变量及配置

### 会话变量

- `cbo_json_v2_rewrite`（默认：true）：启用 JSON v2 路径改写，将 `get_json_*` 等函数改写为直接访问 Flat JSON 子列，从而启用谓词下推和列裁剪。
- `cbo_json_v2_dict_opt`（默认：true）：为路径改写生成的 Flat JSON 字符串子列启用低基数字典优化，可加速字符串表达式、GROUP BY 和 JOIN。

示例：

```SQL
SET cbo_json_v2_rewrite = true;
SET cbo_json_v2_dict_opt = true;
```

### BE 配置

- [json_flat_null_factor](../administration/management/BE_configuration.md#json_flat_null_factor)
- [json_flat_column_max](../administration/management/BE_configuration.md#json_flat_column_max)
- [json_flat_sparsity_factor](../administration/management/BE_configuration.md#json_flat_sparsity_factor)
- [enable_compaction_flat_json](../administration/management/BE_configuration.md#enable_compaction_flat_json)
- [enable_lazy_dynamic_flat_json](../administration/management/BE_configuration.md#enable_lazy_dynamic_flat_json)

## 功能限制

- StarRocks中的所有表模型都支持Flat JSON。
- 兼容历史数据，无需重新导入。历史数据将与Flat JSON扁平化的数据共存。
- 历史数据不会自动应用Flat JSON优化，除非加载新数据或进行Compaction。
- 启用Flat JSON会增加JSON的导入时间，提取的JSON越多，所需时间越长。
- Flat JSON仅支持物化JSON对象中的常用键，不支持JSON数组中的键。
- Flat JSON不改变数据排序方式，因此查询性能和数据压缩率仍会受到数据排序的影响。为了达到最佳性能，可能需要进一步调整数据排序。

## 版本说明

StarRocks存算一体集群从v3.3.0开始支持Flat JSON，存算分离集群从v3.3.3开始支持。

在v3.3.0、v3.3.1和v3.3.2版本中：
- 在加载数据时，支持提取常用字段并单独存储为JSON类型，无需类型推断。
- 提取的列和原始JSON数据都将存储。提取的数据将与原始数据一起删除。

从v3.3.3版本开始：
- Flat JSON提取的结果分为常用列和保留字段列。当所有JSON Schema一致时，不会生成保留字段列。
- Flat JSON仅存储常用字段列和保留字段列，不额外存储原始JSON数据。
- 在加载数据时，常用字段将自动推断为BIGINT/LARGEINT/DOUBLE/STRING类型。无法识别的类型将推断为JSON类型，保留字段列将存储为JSON类型。
<<<<<<< HEAD

## 启用Flat JSON功能（仅支持存算一体集群）

1. 在创建表时，可以在表参数中设置`flat_json.enable`属性。参考[表创建](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md)。
   Flat JSON功能也可以通过直接修改表属性来启用或重新配置。示例：
   ```SQL
   alter table t1 set ("flat_json.enable" = "true")
   
   alter table t1 set ("flat_json.null.factor" = "0.1")
   
   alter table t1 set ("flat_json.sparsity.factor" = "0.8")
   
   alter table t1 set ("flat_json.column.max" = "90")
   ```
2. 启用FE分区裁剪功能：`SET GLOBAL cbo_prune_json_subfield = true;`

## 启用Flat JSON功能（3.4版本之前）

1. 修改BE配置：`enable_json_flat`，在3.4版本之前默认为`false`。修改方法参考
[配置BE参数](../administration/management/BE_configuration.md#configure-be-parameters)
2. 启用FE分区裁剪功能：`SET GLOBAL cbo_prune_json_subfield = true;`

## 其他可选BE配置

- [json_flat_null_factor](../administration/management/BE_configuration.md#json_flat_null_factor)
- [json_flat_column_max](../administration/management/BE_configuration.md#json_flat_column_max)
- [json_flat_sparsity_factor](../administration/management/BE_configuration.md#json_flat_sparsity_factor)
- [enable_compaction_flat_json](../administration/management/BE_configuration.md#enable_compaction_flat_json)
- [enable_lazy_dynamic_flat_json](../administration/management/BE_configuration.md#enable_lazy_dynamic_flat_json)
=======
>>>>>>> 0bbcfda410 ([Doc] Re-organize Flat JSON docs for clarity (#64526))
