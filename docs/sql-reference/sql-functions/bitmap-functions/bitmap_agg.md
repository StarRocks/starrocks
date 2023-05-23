# bitmap_agg

## Description

Aggregates values (excluding `NULL`) in a column into an bitmap (multiple rows to one row).

## Syntax

```Haskell
BITMAP_AGG(col)
```

## Parameters

- `col`: the column whose values you want to aggregate. Supported data types are BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT.

## Return value

Returns a value of the BITMAP type.

## Usage notes

- If the value of param < 0 or > 18446744073709551615, the value will be ignored and not added to the Bitmap.

## Examples

Take the following data table as one example:

```Plain%20Text
mysql> select * from t;
+------+
| c1   |
+------+
|    1 |
|    2 |
|    3 |
+------+
```

Example 1: Aggregate values in column `id` into one bitmap.

```Plain%20Text
mysql> select bitmap_to_string(bitmap_agg(c1)) from t;
+----------------------------------+
| bitmap_to_string(bitmap_agg(c1)) |
+----------------------------------+
| 1,2,3                            |
+----------------------------------+
```

## Keywords

BITMAP_AGG, BITMAP
