---
displayed_sidebar: "Chinese"
---

# reverse

## 功能

将字符串或数组反转，返回的字符串或数组的顺序和源字符串或数组的顺序相反。

## 语法

```Haskell
reverse(param)
```

## 参数说明

`param`：需要反转的字符串或数组，目前只支持一维数组且数组元素的数据类型不允许为 `DECIMAL`。`param` 支持的数据类型为 VARCHAR、CHAR、ARRAY。

数组中的元素支持以下类型：BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、VARCHAR、DECIMALV2、DATETIME、DATE、JSON。**从 2.5 版本开始，该函数支持 JSON 类型的数组元素。**

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
