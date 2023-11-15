# cast

## Description

Converts a value between a JSON data type and an SQL data type.

## Syntax

- Converts a value from a JSON data type to an SQL data type.

```SQL
cast(json_expr AS sql_data_type)
```

- Converts a value from an SQL data type to a JSON data type.

```SQL
cast(sql_expr AS JSON)
```

## Parameters

- `json_expr`: the expression that represents the JSON value you want to convert to an SQL value.

- `sql_data_type`: the SQL data type to which you want to convert the JSON value. Only the STRING, VARCHAR, CHAR, BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DOUBLE, and FLOAT data types are supported.

- `sql_expr`: the expression that represents the SQL value you want to convert to a JSON value. This parameter supports all SQL data types that are supported by the `sql_data_type` parameter.

## Return value

- If you use the `cast(json_expr AS sql_data_type)` syntax, the cast function returns a value of the SQL data type that is specified by the `sql_data_type` parameter.

- If you use the `cast(sql_expr AS JSON)` syntax, the cast function returns a JSON value.

## Usage notes

When you convert an SQL value to a JSON value, take note of the following points:

- If the SQL value exceeds the precision that is supported by JSON, the cast function returns `NULL` to prevent an arithmetic overflow.

- If the SQL value is `NULL`, the cast function does not convert the SQL value `NULL` to a JSON value of `NULL`. The return value is still an SQL value of `NULL`.

When you convert a JSON value to an SQL value, take note of the following points:

- The cast function supports only conversions between compatible JSON and SQL data types. For example, you can convert a JSON string to an SQL string.

- The cast function does not support conversions between incompatible JSON and SQL data types. For example, if you convert a JSON number to an SQL string, the function returns `NULL`.

- If an arithmetic overflow occurs, the cast function returns an SQL value of `NULL`.

- If you convert a JSON value of `NULL` to an SQL value, the function returns an SQL value of `NULL`.

- If you convert a JSON string to a VARCHAR value, the function returns a VARCHAR value that is not enclosed in double quotation marks (").

## Examples

Example 1: Convert a JSON value to an SQL value.

```Plain_Text
-- Convert a JSON value to an INT value.

mysql> select cast(parse_json('1') as int);

+------------------------------+

| cast(parse_json('1') AS INT) |

+------------------------------+

|                            1 |

+------------------------------+



-- Convert a JSON string to a VARCHAR value.

mysql> select cast(parse_json('"star"') as varchar);

+---------------------------------------+

| cast(parse_json('"star"') AS VARCHAR) |

+---------------------------------------+

| star                                  |

+---------------------------------------+



-- Convert a JSON object to a VARCHAR value.

mysql> select cast(parse_json('{"star": 1}') as varchar);

+--------------------------------------------+

| cast(parse_json('{"star": 1}') AS VARCHAR) |

+--------------------------------------------+

| {"star": 1}                                |

+--------------------------------------------+



-- Convert a JSON array to a VARCHAR value.

mysql> select cast(parse_json('[1,2,3]') as varchar);

+----------------------------------------+

| cast(parse_json('[1,2,3]') AS VARCHAR) |

+----------------------------------------+

| [1, 2, 3]                              |

+----------------------------------------+
```

Example 2: Convert an SQL value to a JSON value.

```Plain_Text
-- Convert an INT value to a JSON value.

mysql> select cast(1 as json);

+-----------------+

| cast(1 AS JSON) |

+-----------------+

| 1               |

+-----------------+



-- Convert a VARCHAR value to a JSON value.

mysql> select cast("star" as json);

+----------------------+

| cast('star' AS JSON) |

+----------------------+

| "star"               |

+----------------------+



-- Convert a BOOLEAN value to a JSON value.

mysql> select cast(true as json);

+--------------------+

| cast(TRUE AS JSON) |

+--------------------+

| true               |

+--------------------+
```
