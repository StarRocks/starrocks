# CAST

## 功能

将 `input` 转换成指定数据类型的值，如 `cast (input as BIGINT)` 将当前 `input` 转换为 BIGINT 类型的值。

## 语法

```Haskell
cast(input as type)
```

## 参数说明

`input`: 待转换类型的数据

`type`: 目标数据类型

## 返回值说明

返回值的数据类型为`type`指定的类型。如果转化失败，返回 NULL。

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

示例二：导入中转换原始数据。

```bash
curl --location-trusted -u root: -T ~/user_data/bigint \

    -H "columns: tmp_k1, k1=cast(tmp_k1 as BIGINT)" \

    http://host:port/api/test/bigint/_stream_load
```

> **说明**
>
> 在导入过程中，对于原始数据类型为浮点数的 STRING 做转换时，数据会被转换成 NULL。比如浮点数 12.0 会转为 NULL。如果想强制将这种类型的原始数据转为 BIGINT，需要先将STRING类型的浮点数转为DOUBLE，再转为 BIGINT，请参考如下示例:

```bash
curl --location-trusted -u root: -T ~/user_data/bigint \

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
