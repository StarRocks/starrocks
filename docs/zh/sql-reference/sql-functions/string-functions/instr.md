---
displayed_sidebar: docs
---

# instr

## 功能

返回 substr 在 str 中第一次出现的位置（从 1 开始计数，按「字符」计算）。如果 substr 不在 str 中出现，则返回 0。

## 语法

```Haskell
instr(str, substr)
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

`substr`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 INT。

## 示例

```Plain Text
MySQL > select instr("abc", "b");
+-------------------+
| instr('abc', 'b') |
+-------------------+
|                 2 |
+-------------------+

MySQL > select instr("abc", "d");
+-------------------+
| instr('abc', 'd') |
+-------------------+
|                 0 |
+-------------------+
```
