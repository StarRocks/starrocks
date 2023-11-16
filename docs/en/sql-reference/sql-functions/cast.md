---
displayed_sidebar: "English"
---

# CAST

## Description

Converts an input into the specified type. For example, `cast (input as BIGINT)` converts the input into a BIGINT value.

From v2.4, StarRocks supports conversion to the ARRAY type.

## Syntax

```Haskell
cast (input as type)
```

## Parameters

`input`: the data you want to convert.
`type`: the destination data type.

## Return value

Returns a value whose data type is the same as `type`.

## Examples

Example 1: common data conversions

```Plain Text
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
    
    select cast (1 as BIGINT);
    +-------------------+
    | CAST(1 AS BIGINT) |
    +-------------------+
    |                 1 |
    +-------------------+
```

Example 2: Convert the input into ARRAY.

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

Example 3: Convert data during loading.

```bash
    curl --location-trusted -u <username>:<password> -T ~/user_data/bigint \
        -H "columns: tmp_k1, k1=cast(tmp_k1 as BIGINT)" \
        http://host:port/api/test/bigint/_stream_load
```

> **Note**
>
> If the original value is a floating-point value (such as 12.0), it will be converted to NULL. If you want to forcibly convert this type into BIGINT, see the following example:

```bash
    curl --location-trusted -u <username>:<password> -T ~/user_data/bigint \
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
```
