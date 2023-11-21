---
displayed_sidebar: "Chinese"
---

# uuid_numeric

## 功能

返回一个数值类型的随机 UUID 值。相比`uuid`函数，该函数执行性能提升近2个数量级。

## 语法

```Haskell
uuid_numeric();
```

## 参数说明

无

## 返回值说明

返回 LARGEINT 类型的值。

## 示例

```Plain Text
MySQL > select uuid_numeric();
+--------------------------+
| uuid_numeric()           |
+--------------------------+
| 558712445286367898661205 |
+--------------------------+
1 row in set (0.00 sec)
```
