---
displayed_sidebar: "Chinese"
---

# find_in_set

## 功能

返回 strlist 中第一次出现 str 的位置 (从 1 开始计数)。如果没有找到返回 0，任意参数为 NULL 就返回 NULL。

## 语法

```Haskell
find_in_set(str, strlist)
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

`strlist`: 是用逗号分隔的字符串，支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 INT。

## 示例

```Plain Text
MySQL > select find_in_set("b", "a,b,c");
+---------------------------+
| find_in_set('b', 'a,b,c') |
+---------------------------+
|                         2 |
+---------------------------+
```
