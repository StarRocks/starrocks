---
displayed_sidebar: docs
---

# encode_sort_key

Creates an order-preserving composite binary key from multiple heterogeneous input columns. This function is essential for creating efficient sort keys that maintain the original sort order when compared lexicographically.

## Syntax

```SQL
encode_sort_key(column1, column2, ..., columnN)
```

## Parameters

- `column1, column2, ..., columnN`: One or more columns of any supported data type. The function accepts a variable number of arguments.

## Return value

Returns a value of the VARBINARY type representing the composite sort key.

## Supported data types

The following data types are supported:

| Data Type | Description |
|-----------|-------------|
| `TINYINT` | 8-bit signed integer |
| `SMALLINT` | 16-bit signed integer |
| `INT` | 32-bit signed integer |
| `BIGINT` | 64-bit signed integer |
| `LARGEINT` | 128-bit signed integer |
| `FLOAT` | 32-bit floating point |
| `DOUBLE` | 64-bit floating point |
| `VARCHAR` | Variable-length string |
| `CHAR` | Fixed-length string |
| `DATE` | Date value |
| `DATETIME` | Date and time value |
| `TIMESTAMP` | Timestamp value |

The following complex types are not supported and will return an error:

- `JSON`
- `ARRAY`
- `MAP`
- `STRUCT`
- `HLL`
- `BITMAP`
- `PERCENTILE`

## Encoding strategy

The function uses different encoding strategies for different data types to ensure that lexicographic comparison of the resulting binary keys preserves the original sort order:

- **Integer types**: Big-endian byte order with sign bit flipped for signed types
- **Floating-point types**: Custom encoding to ensure correct sort order
- **String types**: Special encoding with 0x00 byte escaping and 0x00 0x00 terminator for non-last fields
- **Date/Time types**: Internal integer representation encoded as integral value

### NULL handling

- Each column gets a NULL marker (0x00 for NULL, 0x01 for NOT NULL) for every row
- NULL markers are added even for non-nullable columns to ensure consistent encoding
- A separator byte (0x00) is used between columns (except after the last column)

## Examples

### Generated sort key column

```sql
CREATE TABLE user_analytics (
    user_id INT,
    region VARCHAR(50),
    score DOUBLE,
    created_date DATE,
    sort_key VARBINARY(1024) AS (
        encode_sort_key(region, score, created_date)
    )
) ORDER BY (sort_key);
```

### JSON field extraction

```sql
CREATE TABLE json_data (
    id INT,
    json_content JSON,
    sort_key VARBINARY(1024) AS (
        encode_sort_key(
            get_json_int(json_content, '$.priority'),
            get_json_string(json_content, '$.category'),
            get_json_double(json_content, '$.score')
        )
    )
) ORDER BY (sort_key);
```

## Limitations

### Data type restrictions

Complex types like JSON, ARRAY, MAP, and STRUCT cannot be directly encoded. Use JSON extraction functions to get primitive values:

```sql
-- Instead of: encode_sort_key(json_col)
-- Use: encode_sort_key(get_json_int(json_col, '$.field1'), get_json_string(json_col, '$.field2'))
```

### Performance considerations

- Each call to `encode_sort_key` requires encoding all input columns
- Binary keys can be significantly larger than original data
- Use generated columns to avoid repeated encoding

### Key size limitations

- Keep the number of columns reasonable (typically < 10)
- Prefer shorter string columns when possible
- Consider using hash functions for very long strings