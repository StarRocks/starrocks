---
displayed_sidebar: docs
---

# find_in_set

この関数は、strlist 内で最初に出現する str の位置を返します（1 からカウント開始）。strlist はカンマで区切られた文字列です。str が見つからない場合は 0 を返します。引数が NULL の場合、結果は NULL です。

## Syntax

```Haskell
INT find_in_set(VARCHAR str, VARCHAR strlist)
```

## Examples

```Plain Text
MySQL > select find_in_set("b", "a,b,c");
+---------------------------+
| find_in_set('b', 'a,b,c') |
+---------------------------+
|                         2 |
+---------------------------+
```

## keyword

FIND_IN_SET,FIND,IN,SET