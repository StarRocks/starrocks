---
displayed_sidebar: "Chinese"
---

# reverse

## 功能

将字符串/数组反转，返回的字符串/数组的顺序和源字符串/数组的顺序相反。

## 语法

```Haskell
reverse(param)
```

## 参数说明

`param`：需要反转的字符串/数组，目前只支持一维数组且数组元素的数据类型不允许为 `DECIMAL`。支持的数据类型为 VARCHAR、CHAR、ARRAY。

## 返回值说明

返回的数据类型与 `param` 一致。

## 示例

反转字符串。

```Plain Text
MySQL > SELECT REVERSE('hello');
+------------------+
| REVERSE('hello') |
+------------------+
| olleh            |
+------------------+

MySQL > SELECT REVERSE('你好');
+------------------+
| REVERSE('你好')  |
+------------------+
| 好你             |
+------------------+
```

反转数组。

```Plain Text
MYSQL> SELECT REVERSE([4,1,5,8]);
+--------------------+
| REVERSE([4,1,5,8]) |
+--------------------+
| [8,5,1,4]          |
+--------------------+
```

## keyword

REVERSE, ARRAY
