---
displayed_sidebar: docs
---

# element_at

## Description

Returns the value for the specified key from a key-value pair of a map. If any input parameter is NULL or if the key does not exist in the map, the result is NULL.

If you want to retrieve an element from an array, see [element_at](../array-functions/element_at.md).

This function is supported from v3.0 onwards.

## Syntax

```Haskell
element_at(any_map, any_key)
```

## Parameters

- `any_map`: a MAP expression from which to retrieve values.
- `any_key`: a key in the map.

## Return value

If `any_key` exists in `any_map`, the value corresponding to the key will be returned. Otherwise, NULL is returned.

## Examples

```plain text
mysql> select element_at(map{1:3,2:4},1);
+-----------------+
| map{1:3,2:4}[1] |
+-----------------+
|               3 |
+-----------------+

mysql> select element_at(map{1:3,2:4},3);
+-----------------+
| map{1:3,2:4}[3] |
+-----------------+
|            NULL |
+-----------------+

mysql> select element_at(map{'a':1,'b':2},'a');
+-----------------------+
| map{'a':1,'b':2}['a'] |
+-----------------------+
|                     1 |
+-----------------------+
```

## keywords

ELEMENT_AT, MAP
