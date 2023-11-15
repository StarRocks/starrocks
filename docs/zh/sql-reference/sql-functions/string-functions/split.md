# split

## description

### Syntax

```Haskell
`ARRAY<VARCHAR>` split(VARCHAR content, VARCHAR delimiter)
```

根据分隔符拆分字符串，将拆分后的所有字符串以ARRAY的格式返回

## example

```Plain Text
mysql> select split("a,b,c",",");
+---------------------+
| split('a,b,c', ',') |
+---------------------+
| ["a","b","c"]       |
+---------------------+
mysql> select split("a,b,c",",b,");
+-----------------------+
| split('a,b,c', ',b,') |
+-----------------------+
| ["a","c"]             |
+-----------------------+
mysql> select split("abc","");
+------------------+
| split('abc', '') |
+------------------+
| ["a","b","c"]    |
+------------------+
```

## keyword

SPLIT,PART
