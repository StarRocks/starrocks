---
displayed_sidebar: docs
sidebar_position: 110
---

# [Preview] Flat JSON

This article introduces the basic concept of Flat JSON and how to use this feature.

Starting from version 2.2.0, StarRocks supports the JSON data type to allow more flexible data storage. However, when querying JSON, most scenarios do not involve directly reading the entire JSON data but rather accessing data at specified paths. For example:

```SQL
-- Store required fields in logs as fixed fields, and package other fields that frequently change with business as JSON.
SELECT
    time,
    event,
    user,
    get_json_string(remain_json, "$.from_system"),
    get_json_string(remain_json, "$.tag")
FROM logs;
```

Due to the special nature of the JSON type, its performance in queries is not as good as standard types (INT, STRING, etc.). The reasons include:
- Storage overhead: JSON is a semi-structured type that requires storing the structure information of each row, leading to high storage usage and low compression efficiency.
- Query complexity: Queries need to detect data structures based on runtime data, making it difficult to achieve vectorized execution optimization.
- Redundant data: Queries need to read the entire JSON data, which includes many redundant fields.

StarRocks introduces the Flat JSON feature to improve JSON data query efficiency and reduce the complexity of using JSON.
- This feature is available from version 3.3.0, disabled by default, and needs to be enabled manually.
- From version 3.4.0, the Flat JSON feature is enabled by default, requiring no manual operation.

## What is Flat JSON

The core principle of Flat JSON is to detect JSON data during load and extract common fields from JSON data for storage as standard type data. When querying JSON, these common fields optimize the query speed of JSON. Example data:

```Plaintext
1, {"a": 1, "b": 21, "c": 3, "d": 4}
2, {"a": 2, "b": 22, "d": 4}
3, {"a": 3, "b": 23, "d": [1, 2, 3, 4]}
4, {"a": 4, "b": 24, "d": null}
5, {"a": 5, "b": 25, "d": null}
6, {"c": 6, "d": 1}
```

When loading the above JSON data, fields `a` and `b` are present in most JSON data and have similar data types (both INT). Therefore, the data of fields `a` and `b` can be extracted from JSON and stored separately as two INT columns. When these two columns are used in queries, their data can be directly read without needing to process additional JSON fields, reducing the computational overhead of handling JSON structures.

## Verify if Flat JSON is Effective

After loading data, you can query the extracted sub-columns of the corresponding column:

```SQL
SELECT flat_json_meta(<json_column>)
FROM <table_name>[_META_];
```

You can verify whether the executed query benefits from Flat JSON optimization through the [Query Profile](../administration/query_profile_overview.md) by observing the following metrics:
- `PushdownAccessPaths`: The number of sub-field paths pushed down to storage.
- `AccessPathHits`: The number of times Flat JSON sub-fields are hit, with detailed information on the specific JSON hit.
- `AccessPathUnhits`: The number of times Flat JSON sub-fields are not hit, with detailed information on the specific JSON not hit.
- `JsonFlattern`: The time taken to extract sub-columns on-site when Flat JSON is not hit.

## Usage Example

1. Enable the feature (refer to other sections)
2. Create a table with JSON columns. In this example, use INSERT INTO to load JSON data into the table.

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

3. View the extracted sub-columns for the `k2` column.

   ```Plaintext
   SELECT flat_json_meta(k2) FROM t1[_META_];
   +---------------------------------------------------------------------------------------------------------------------------+
   | flat_json_meta(k2)                                                                                                        |
   +---------------------------------------------------------------------------------------------------------------------------+
   | ["nulls(TINYINT)","Integer(BIGINT)","Double(DOUBLE)","str(VARCHAR)","Bool(JSON)","Object(JSON)","arr(JSON)","null(JSON)"] |
   +---------------------------------------------------------------------------------------------------------------------------+
   ```

5. Execute data queries.

   ```SQL
   SELECT * FROM t1;
   SELECT get_json_string(k2,'\$.Integer') FROM t1 WHERE k2->'str' = 'test_flat_json';
   SELECT get_json_string(k2,'\$.Double') FROM t1 WHERE k2->'Integer' = 123456;
   SELECT get_json_string(k2,'\$.Object') FROM t1 WHERE k2->'Double' = 3.14158;
   SELECT get_json_string(k2,'\$.arr') FROM t1 WHERE k2->'Object' = to_json(map{'c':'d'});
   SELECT get_json_string(k2,'\$.Bool') FROM t1 WHERE k2->'arr' = '[10,20,30]';
   ```

7. View Flat JSON-related metrics in the [Query Profile](../administration/query_profile_overview.md)
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

## Feature Limitations

- All table types in StarRocks support Flat JSON.
- Compatible with historical data without needing re-import. Historical data will coexist with data flattened by Flat JSON.
- Historical data will not automatically apply Flat JSON optimization unless new data is loaded or Compaction occurs.
- Enabling Flat JSON will increase the time taken to load JSON. The more JSON extracted, the longer it takes.
- Flat JSON can only support materializing common keys in JSON Objects, not keys in JSON Arrays.
- Flat JSON does not change the data sorting method, so query performance and data compression rate will still be affected by data sorting. To achieve optimal performance, further adjustments to data sorting may be necessary.

## Version Notes

StarRocks shared-nothing clusters support Flat JSON starting from v3.3.0, and shared-data clusters support it from v3.3.3.

In versions v3.3.0, v3.3.1, and v3.3.2:
- When loading data, it supports extracting common fields and storing them separately as JSON types, without type inference.
- Both the extracted columns and the original JSON data are stored. The extracted data will be deleted along with the original data.

Starting from version v3.3.3:
- The results extracted by Flat JSON are divided into common columns and reserved field columns. When all JSON Schemas are consistent, no reserved field columns are generated.
- Flat JSON only stores common field columns and reserved field columns, without additionally storing the original JSON data.
- When loading data, common fields will automatically infer types as BIGINT/LARGEINT/DOUBLE/STRING. Unrecognized types will be inferred as JSON types, and reserved field columns will be stored as JSON types.

## Enabling Flat JSON Feature (Before Version 3.4)

1. Modify BE configuration: `enable_json_flat`, which defaults to `false` before version 3.4. For modification methods, refer to
[Configure BE parameters](../administration/management/BE_configuration.md#configure-be-parameters)
2. Enable FE pruning feature: `SET GLOBAL cbo_prune_json_subfield = true;`

## Other Optional BE Configurations

- [json_flat_null_factor](../administration/management/BE_configuration.md#json_flat_null_factor)
- [json_flat_column_max](../administration/management/BE_configuration.md#json_flat_column_max)
- [json_flat_sparsity_factor](../administration/management/BE_configuration.md#json_flat_sparsity_factor)
- [enable_compaction_flat_json](../administration/management/BE_configuration.md#enable_compaction_flat_json)
- [enable_lazy_dynamic_flat_json](../administration/management/BE_configuration.md#enable_lazy_dynamic_flat_json)
