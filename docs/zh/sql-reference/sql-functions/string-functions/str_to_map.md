# str_to_map

## 功能

将给定的字符串分割成键值对 (Key-Value pair)，返回包含这些键值对的 Map。

该函数从 3.1 版本开始支持。

## 语法

```Haskell
MAP<VARCHAR, VARCHAR> str_to_map(VARCHAR content[, VARCHAR delimiter[, VARCHAR map_delimiter]])
```

## 参数说明

- `content`: 待分割的字符串表达式，必选。
- `delimiter`: 用于将字符串分割成键值对，可选。如果不指定，默认为逗号 (`,`)。
- `map_delimiter`: 用于将每个键值对分开，可选。如果不指定，默认为冒号 (`:`)。

## 返回值说明

返回元素为 VARCHAR 的 MAP。如果任何一个输入值为 NULL，则返回 NULL。

## 示例

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
