---
displayed_sidebar: "Chinese"
---

# starts_with

## 功能

如果字符串以指定前缀开头返回 1，否则返回 0，任意参数为 NULL 则返回 NULL。

## 语法

```Haskell
starts_with(str, prefix)
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

`perfix`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 BOOLEAN。

## 示例

```Plain Text
mysql> select starts_with("hello world","hello");
+-------------------------------------+
|starts_with('hello world', 'hello')  |
+-------------------------------------+
| 1                                   |
+-------------------------------------+

mysql> select starts_with("hello world","world");
+-------------------------------------+
|starts_with('hello world', 'world')  |
+-------------------------------------+
| 0                                   |
+-------------------------------------+
```
