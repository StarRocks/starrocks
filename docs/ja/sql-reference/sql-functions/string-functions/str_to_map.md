---
displayed_sidebar: docs
---

# str_to_map

## Description

指定された文字列を2つのデリミタでキーと値のペアに分割し、分割されたペアのマップを返します。

この関数はv3.1以降でサポートされています。

## Syntax

```Haskell
MAP<VARCHAR, VARCHAR> str_to_map(VARCHAR content[, VARCHAR delimiter[, VARCHAR map_delimiter]])
```

## Parameters

- `content`: 必須、分割する文字列式。
- `delimiter`: オプション、`content`をキーと値のペアに分割するために使用されるデリミタ。デフォルトは `,`。
- `map_delimiter`: オプション、各キーと値のペアを分けるために使用されるデリミタ。デフォルトは `:`。

## Return value

STRING要素のMAPを返します。入力がNULLの場合、結果はNULLになります。

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