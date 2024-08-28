---
displayed_sidebar: docs
---

# map_from_arrays

## Description

Creates a MAP value from the given pair of key item array and value item array.

This function is supported from v3.1 onwards.

## Syntax

```Haskell
MAP map_from_arrays(ARRAY keys, ARRAY values)
```

## Parameters

- `keys`: uses to construct the keys of the result MAP. Make sure the elements of keys are unique.
- `values`: used to construct the values of the result MAP.

## Return value

Returns a MAP that is constructed from the input keys and values.

- Keys and values must have the same length, otherwise it will return an error.
- If key or value is NULL, this function returns NULL.
- The returned MAP value has distinct keys.

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

select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']);
+--------------------------------------------------------------------------+
| map_from_arrays([1, 3, NULL, 2, NULL], ['ab', 'cdd', NULL, NULL, 'abc']) |
+--------------------------------------------------------------------------+
| {1:"ab",3:"cdd",2:null,null:"abc"}                                       |
+--------------------------------------------------------------------------+
```
