---
displayed_sidebar: docs
---

# str_to_map

指定された文字列を2つの区切り文字でキーと値のペアに分割し、分割されたペアのマップを返します。

この関数は v3.1 以降でサポートされています。

## Syntax

```Haskell
MAP<VARCHAR, VARCHAR> str_to_map(VARCHAR content[, VARCHAR delimiter[, VARCHAR map_delimiter]])
```

## Parameters

- `content`: 必須、分割する文字列式。
- `delimiter`: 任意、`content` をキーと値のペアに分割するための区切り文字。デフォルトは `,`。
- `map_delimiter`: 任意、各キーと値のペアを分けるための区切り文字。デフォルトは `:`。

## Return value

STRING 要素の MAP を返します。入力が NULL の場合は NULL を返します。

## Examples

```SQL
mysql> SELECT str_to_map('a:1|b:2|c:3', '|', ':') as map;
+---------------------------+
| map                       |
+---------------------------+
| {"a":"1","b":"2","c":"3"} |
+---------------------------+

mysql> SELECT str_to_map('a:1;b:2;c:3', ';', ':') as map;
+---------------------------+
| map                       |
+---------------------------+
| {"a":"1","b":"2","c":"3"} |
+---------------------------+

mysql> SELECT str_to_map('a:1,b:2,c:3', ',', ':') as map;
+---------------------------+
| map                       |
+---------------------------+
| {"a":"1","b":"2","c":"3"} |
+---------------------------+

mysql> SELECT str_to_map('a') as map;
+------------+
| map        |
+------------+
| {"a":null} |
+------------+

mysql> SELECT str_to_map('a:1,b:2,c:3',null, ':') as map;
+------+
| map  |
+------+
| NULL |
+------+

mysql> SELECT str_to_map('a:1,b:2,c:null') as map;
+------------------------------+
| map                          |
+------------------------------+
| {"a":"1","b":"2","c":"null"} |
+------------------------------+
```

## keywords

STR_TO_MAP