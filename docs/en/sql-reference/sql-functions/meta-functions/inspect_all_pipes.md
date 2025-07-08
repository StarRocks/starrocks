---
displayed_sidebar: docs
---

# inspect_all_pipes

`inspect_all_pipes()`

This function returns meta data of all pipes in the current database.

## Arguments

None.

## Return Value

Returns a VARCHAR string containing the meta data of all pipes in JSON format.


## Examples

Example 1: Get the curent all 

```
mysql> select inspect_all_pipes();
+---------------------+
| inspect_all_pipes() |
+---------------------+
| []                  |
+---------------------+
1 row in set (0.01 sec)
```