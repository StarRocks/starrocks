# json_object

## Description

Converts one or more key-value pairs to a JSON object that consists of the key-value pairs. The key-value pairs are sorted by key in dictionary order.

## Syntax

```Haskell
json_object(key, value, ...)
```

## Parameters

- `key`: a key in the JSON object. Only the VARCHAR data type is supported.

- `value`: a value in the JSON object. Only `NULL` values and the following data types are supported: STRING, VARCHAR, CHAR, JSON, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DOUBLE, FLOAT, and BOOLEAN.

## Return value

Returns a JSON object.

> If the total number of keys and values is an odd number, the JSON_OBJECT function fills `NULL` in the last field.

## Examples

Example 1: Construct a JSON object that consists of values of different data types.

```Plain%20Text
mysql> SELECT json_object('name', 'starrocks', 'active', true, 'published', 2020);

       -> {"active": true, "name": "starrocks", "published": 2020}            
```

Example 2: Construct a JSON object by using nested JSON_OBJECT functions.

```Plain%20Text
mysql> SELECT json_object('k1', 1, 'k2', json_object('k2', 2), 'k3', json_array(4, 5));

       -> {"k1": 1, "k2": {"k2": 2}, "k3": [4, 5]} 
```

Example 3: Construct an empty JSON object.

```Plain%20Text
mysql> SELECT json_object();

       -> {}
```
