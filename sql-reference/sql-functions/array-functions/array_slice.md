# array_slice

## Description

Returns a slice of an array.

## Syntax

```SQL
array_slice(input, offset, length)
```

## Parameters

- `input`: the array whose slice you want to obtain.

- `offset`: the offset at which the slice that you want to obtain starts. Valid values start from `1`.

- `length`: the length of the slice that you want to obtain.

## Return value

Returns an array of the same data type as the array specified by the `input` parameter.

## Examples

Example 1:

```Plain
mysql> select array_slice([1,2,4,5,6], 3, 2) as res;
+-------+
| res   |
+-------+
| [4,5] |
+-------+
```

Example 2:

```Plain
mysql> select array_slice(["sql","storage","query","execute"], 1, 2) as res;
+-------------------+
| res               |
+-------------------+
| ["sql","storage"] |
+-------------------+
```

Example 3:

```Plain
mysql> select array_slice([57.73,97.32,128.55,null,324.2], 3, 3) as res;
+---------------------+
| res                 |
+---------------------+
| [128.55,null,324.2] |
+---------------------+
```

Example 4:

```Plain
mysql> select array_slice([57.73,97.32,128.55,null,324.2], 3, 5) as res;
+---------------------+
| res                 |
+---------------------+
| [128.55,null,324.2] |
+---------------------+
```
