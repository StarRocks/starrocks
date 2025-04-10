---
displayed_sidebar: docs
---

# CAST

## 功能

将 `input` 转换成指定数据类型的值，如 `cast (input as BIGINT)` 将当前 `input` 转换为 BIGINT 类型的值。

从 2.4 版本开始支持将 Array 字符串 和 JSON array 转换为 ARRAY 类型。

## 语法

```Haskell
cast(input as type)
```

## 参数说明

`input`: 待转换类型的数据

`type`: 目标数据类型

## 返回值说明

返回值的数据类型为 `type` 指定的类型。如果转化失败，返回 NULL。

## 示例

示例一：常见转换。

```Plain Text
    select cast (1 as BIGINT);
    +-------------------+
    | CAST(1 AS BIGINT) |
    +-------------------+
    |                 1 |
    +-------------------+

    select cast('9.5' as DECIMAL(10,2));
    +--------------------------------+
    | CAST('9.5' AS DECIMAL64(10,2)) |
    +--------------------------------+
    |                           9.50 |
    +--------------------------------+

    select cast(NULL AS INT);
    +-------------------+
    | CAST(NULL AS INT) |
    +-------------------+
    |              NULL |
    +-------------------+

    select cast(true AS BOOLEAN);
    +-----------------------+
    | CAST(TRUE AS BOOLEAN) |
    +-----------------------+
    |                     1 |
    +-----------------------+
```

示例二：转换为 ARRAY 类型。

```Plain Text
    -- Convert string to ARRAY<ANY>.

    select cast('[1,2,3]' as array<int>);
    +-------------------------------+
    | CAST('[1,2,3]' AS ARRAY<INT>) |
    +-------------------------------+
    | [1,2,3]                       |
    +-------------------------------+

    select cast('[1,2,3]' as array<bigint>);
    +----------------------------------+
    | CAST('[1,2,3]' AS ARRAY<BIGINT>) |
    +----------------------------------+
    | [1,2,3]                          |
    +----------------------------------+

    select cast('[1,2,3]' as array<string>);
    +------------------------------------------+
    | CAST('[1,2,3]' AS ARRAY<VARCHAR(65533)>) |
    +------------------------------------------+
    | ["1","2","3"]                            |
    +------------------------------------------+

    select cast('["a", "b", "c"]' as array<string>);
    +--------------------------------------------------+
    | CAST('["a", "b", "c"]' AS ARRAY<VARCHAR(65533)>) |
    +--------------------------------------------------+
    | ["a","b","c"]                                    |
    +--------------------------------------------------+

    -- Convert JSON array to ARRAY<ANY>.

    select cast(parse_json('[{"a":1}, {"a": 2}]')  as array<json>);
    +----------------------------------------------------------+
    | CAST((parse_json('[{"a":1}, {"a": 2}]')) AS ARRAY<JSON>) |
    +----------------------------------------------------------+
    | ['{"a": 1}','{"a": 2}']                                  |
    +----------------------------------------------------------+

    select cast(parse_json('[1, 2, 3]')  as array<int>);
    +-----------------------------------------------+
    | CAST((parse_json('[1, 2, 3]')) AS ARRAY<INT>) |
    +-----------------------------------------------+
    | [1,2,3]                                       |
    +-----------------------------------------------+

    select cast(parse_json('["1","2","3"]') as array<string>);
    +--------------------------------------------------------------+
    | CAST((parse_json('["1","2","3"]')) AS ARRAY<VARCHAR(65533)>) |
    +--------------------------------------------------------------+
    | ["1","2","3"]                                                |
    +--------------------------------------------------------------+
```

示例三：导入中转换原始数据。

```bash
curl --location-trusted -u <username>:<password> -T ~/user_data/bigint \

    -H "columns: tmp_k1, k1=cast(tmp_k1 as BIGINT)" \

    http://host:port/api/test/bigint/_stream_load
```

> **说明**
>
> 在导入过程中，对于原始数据类型为浮点数的 STRING 做转换时，数据会被转换成 NULL。比如浮点数 12.0 会转为 NULL。如果想强制将这种类型的原始数据转为 BIGINT，需要先将STRING类型的浮点数转为DOUBLE，再转为 BIGINT，请参考如下示例:

```bash
curl --location-trusted -u <username>:<password> -T ~/user_data/bigint \

    -H "columns: tmp_k1, k1=cast(cast(tmp_k1 as DOUBLE) as BIGINT)" \

    http://host:port/api/test/bigint/_stream_load
```

```plain text
select cast(cast ("11.2" as double) as bigint);
+----------------------------------------+
| CAST(CAST('11.2' AS DOUBLE) AS BIGINT) |
+----------------------------------------+
|                                     11 |
+----------------------------------------+
1 row in set (0.00 sec)
```
