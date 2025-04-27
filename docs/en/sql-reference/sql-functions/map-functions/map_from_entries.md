---
displayed_sidebar: docs
---

# map_from_entries



Creates a MAP value from the given array of entries that contains just 2 fields.

This function is supported from v3.4 onwards.

## Syntax

```Haskell
MAP map_from_entries(ARRAY entries)
```

## Parameters

- `entries`: array of structs. Make sure the structs just have two fields.

## Return value

Returns a MAP that is constructed from the structs.

## Examples

```Plaintext
select map_from_entries([row(1, null)]);
+----------------------------------+
| map_from_entries([row(1, NULL)]) |
+----------------------------------+
| {1:null}                         |
+----------------------------------+

select map_from_entries([row(1, 'a'), row(2,'b'), row(3, 'c')]);+-----------------------------------------------------------+
| map_from_entries([row(1, 'a'), row(2, 'b'), row(3, 'c')]) |
+-----------------------------------------------------------+
| {1:"a",2:"b",3:"c"}                                       |
+-----------------------------------------------------------+
```
