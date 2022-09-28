# left

## 功能

返回具有指定长度的字符串的左边部分，长度的单位为「utf8 字符」。

## 语法

```Haskell
left(str, len)
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

`len`: 支持的数据类型为 INT。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
MySQL > select left("Hello starrocks",5);
+------------------------+
| left('Hello starrocks', 5) |
+------------------------+
| Hello                  |
+------------------------+
```
