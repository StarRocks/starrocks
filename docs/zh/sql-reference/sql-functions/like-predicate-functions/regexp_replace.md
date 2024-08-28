---
displayed_sidebar: docs
---

# regexp_replace

## 功能

对字符串 str 进行正则匹配，将命中 pattern 的部分使用 repl 来进行替换。

### 语法

```Haskell
regexp_replace(str, pattern, repl)
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

`pattern`: 支持的数据类型为 VARCHAR。

`repl`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
MySQL > SELECT regexp_replace('a b c', " ", "-");
+-----------------------------------+
| regexp_replace('a b c', ' ', '-') |
+-----------------------------------+
| a-b-c                             |
+-----------------------------------+

MySQL > SELECT regexp_replace('a b c','(b)','<\\1>');
+----------------------------------------+
| regexp_replace('a b c', '(b)', '<\1>') |
+----------------------------------------+
| a <b> c                                |
+----------------------------------------+
```
