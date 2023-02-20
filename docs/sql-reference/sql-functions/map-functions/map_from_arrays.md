# map_from_arrays

## Description

Creates a MAP value from the given pair of key item array and value item array.

## Syntax

```Haskell
MAP map_from_arrays(ARRAY keys, ARRAY values)
```

## Parameters

- `keys`: uses to construct the keys of the result MAP. Make sure the elements of keys are unique.
- `values`: used to construct the values of the result MAP.

## Return value

Returns a MAP that is constructed from the input keys and values.

keys and values must have the same length, otherwise it will return an error.
If keys or values are NULL, this function will return NULL.

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
