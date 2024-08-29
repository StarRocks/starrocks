---
displayed_sidebar: docs
---

# array_slice

## Description

Returns a slice of an array. This function intercepts `length` elements from `input` from the position specified by `offset`.

## Syntax

```Haskell
array_slice(input, offset, length)
```

## Parameters

- `input`: the array whose slice you want to intercept. This function supports the following types of array elements: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, VARCHAR, DECIMALV2, DATETIME, DATE, and JSON. **JSON is supported from 2.5.**

- `offset`: the position from which to intercept elements. Valid values start from `1`. It must be a BIGINT value.

- `length`: the length of the slice that you want to intercept. It must be a BIGINT value.

## Return value

Returns an array that have the same data type as the array specified by the `input` parameter.

## Usage notes

- The offset starts from 1.
- If the specified length exceeds the actual number of elements that can be intercepted, all the matching elements are returned. See Example 4.

## Examples

Example 1: Intercept 2 elements starting from the third element.

```Plain
mysql> select array_slice([1,2,4,5,6], 3, 2) as res;
+-------+
| res   |
+-------+
| [4,5] |
+-------+
```

Example 2: Intercept 2 elements starting from the first element.

```Plain
mysql> select array_slice(["sql","storage","query","execute"], 1, 2) as res;
+-------------------+
| res               |
+-------------------+
| ["sql","storage"] |
+-------------------+
```

Example 3: Null elements are treated as normal values.

```Plain
mysql> select array_slice([57.73,97.32,128.55,null,324.2], 3, 3) as res;
+---------------------+
| res                 |
+---------------------+
| [128.55,null,324.2] |
+---------------------+
```

Example 4: Intercept 5 elements starting from the third element.

This function intends to intercept 5 elements but there are only 3 elements from the third element. As a result, all of these 3 elements are returned.

```Plain
mysql> select array_slice([57.73,97.32,128.55,null,324.2], 3, 5) as res;
+---------------------+
| res                 |
+---------------------+
| [128.55,null,324.2] |
+---------------------+
```
