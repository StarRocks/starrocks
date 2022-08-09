# CAST

## 功能

将 input 转换成指定类型的值, 如 `cast (input as BIGINT)` 将当前列 input 转换为 BIGINT 类型的值。

## 语法

```Haskell
cast(input as type)
```

## 参数说明

`input`: 待转换类型的参数

`type`: 目标类型

## 返回值说明

返回值的数据类型为 `type`指定的类型。

## 示例

1. 转换常量，或表中某列。

    ```Plain Text
    MySQL > select cast (1 as BIGINT);
    +-------------------+
    | CAST(1 AS BIGINT) |
    +-------------------+
    |                 1 |
    +-------------------+
    ```

2. 转换导入的原始数据。

    ```bash
    curl --location-trusted -u root: -T ~/user_data/bigint \
        -H "columns: tmp_k1, k1=cast(tmp_k1 as BIGINT)" \
        http://host:port/api/test/bigint/_stream_load
    ```

    > 注：在导入中, 由于原始类型均为 STRING, 将值为浮点的原始数据做 cast 时数据会被转换成 NULL , 比如 12.0 StarRocks 目前不会对原始数据做截断。

    如果想强制将这种类型的原始数据 cast to int 的话, 请参考:

    ```bash
    curl --location-trusted -u root: -T ~/user_data/bigint \
        -H "columns: tmp_k1, k1=cast(cast(tmp_k1 as DOUBLE) as BIGINT)" \
        http://host:port/api/test/bigint/_stream_load
    ```

    ```plain text
    MySQL > select cast(cast ("11.2" as double) as bigint);
    +----------------------------------------+
    | CAST(CAST('11.2' AS DOUBLE) AS BIGINT) |
    +----------------------------------------+
    |                                     11 |
    +----------------------------------------+
    1 row in set (0.00 sec)
    ```
