---
displayed_sidebar: docs
---

# array_append

配列の末尾に新しい要素を追加します。配列を返します。

## Syntax

```Haskell
array_append(any_array, any_element)
```

## Examples

```plain text
mysql> select array_append([1, 2], 3);
+------------------------+
| array_append([1,2], 3) |
+------------------------+
| [1,2,3]                |
+------------------------+
1 row in set (0.00 sec)

```

NULL を配列に追加することができます。

```plain text
mysql> select array_append([1, 2], NULL);
+---------------------------+
| array_append([1,2], NULL) |
+---------------------------+
| [1,2,NULL]                |
+---------------------------+
1 row in set (0.01 sec)

```

## keyword

ARRAY_APPEND,ARRAY