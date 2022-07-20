# CAST

## description

### Syntax

```Haskell
cast (input as type)
```

Convert input to the specified (type) value. For example, `cast (input as BIGINT)`converts the current column input to a BIGINT type value.

## example

1. Convert a constant, or a column in a table.

    ```Plain Text
    MySQL > select cast (1 as BIGINT);
    +-------------------+
    | CAST(1 AS BIGINT) |
    +-------------------+
    |                 1 |
    +-------------------+
    ```

2. Convert imported raw data

    ```bash
    curl --location-trusted -u root: -T ~/user_data/bigint \
        -H "columns: tmp_k1, k1=cast(tmp_k1 as BIGINT)" \
        http://host:port/api/test/bigint/_stream_load
    ```

    > Note: during import, since the original type is String, the data will be converted to NULL when cast the original data with floating point value, such as 12.0. StarRocks will not truncate the original data at present.

    If you want to force this type of raw data to be cast to int. Look at the following:

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

## keyword

CAST
