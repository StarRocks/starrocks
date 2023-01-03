# map_from_arrays

## Description

Create a MAP value from the given pair of key item array and value item array.

## Syntax

```
MAP map_from_arrays(ARRAY keys, ARRAY values)
```

## Parameters

`keys`: To construct the keys of the result MAP. Now users must make sure the elements of keys are unique.
`values`: To construct the values of the result MAP.

## Return value

Return a MAP which is consisted from the input keys and values.

keys and values must have the same length, otherwise it will return an error.
If keys or values is NULL, this function will return NULL.

## Examples

```Plaintext
select map_from_arrays([1, 2], ['Star', 'Rocks']);
+--------------------------------------------+
| map_from_arrays([1, 2], ['Star', 'Rocks']) |
+--------------------------------------------+
| {1:"Star",2:"Rocks"}                       |
+--------------------------------------------+
```

```Plaintext
select map_from_arrays([1, 2], NULL);
+-------------------------------+
| map_from_arrays([1, 2], NULL) |
+-------------------------------+
| NULL                          |
+-------------------------------+
```
