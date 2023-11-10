# map_concat

## Description

Returns the union of the input maps. If a key is found in multiple maps, this function keeps only the last value among these maps, called LAST WIN. For example, `SELECT map_concat(map{1:3},map{1:'4'});` returns `{1:"4"}`.

This function is supported from v3.1 onwards.

## Syntax

```Haskell
ANY_MAP map_concat(any_map0, any_map1...)
```

## Parameters

`any_mapN`: the map values you want to union. All maps must share a common type. If data types of the input maps are not the same, the return type is the common supertype of the input maps.

## Return value

Returns a MAP of the common supertype of the input maps.

## Examples

```Plain
mysql> SELECT map_concat(map(1, 'a', 2, 'b'), map(3, 'c'));
+------------------------------------------+
| map_concat(map{1:'a',2:'b'}, map{3:'c'}) |
+------------------------------------------+
| {3:"c",1:"a",2:"b"}                      |
+------------------------------------------+

mysql> select map_concat(map{1:3},map{'3.323':3});
+----------------------------------+
| map_concat((1, 3), ('3.323', 3)) |
+----------------------------------+
| {"3.323":3,"1":3}                |
+----------------------------------+
1 row in set (0.19 sec)


mysql> select map_concat(map{1:3},map{1:'4', 3:'5',null:null}, null);
+--------------------------------------------------------+
| map_concat((1, 3), (1, '4', 3, '5', NULL, NULL), NULL) |
+--------------------------------------------------------+
| {1:"4",3:"5",null:null}                                |
+--------------------------------------------------------+
1 row in set (0.01 sec)
```
