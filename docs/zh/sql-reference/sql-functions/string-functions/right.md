---
displayed_sidebar: docs
---

# right

## 功能

从字符串右边部分返回指定长度的字符，长度的单位为「utf8 字符」。函数别名为 [strright](strright.md)。

## 语法

```Haskell
VARCHAR right(VARCHAR str, INT len)
```

## 参数说明

`str`: 待处理的字符串，支持的数据类型为 VARCHAR。

`len`: 要返回的字符长度，支持的数据类型为 INT。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
MySQL > select right("Hello starrocks",5);
+-------------------------+
| right('Hello starrocks', 5) |
+-------------------------+
| rocks                   |
+-------------------------+
```
