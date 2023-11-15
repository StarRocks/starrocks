# ends_with

## 功能

如果字符串以指定后缀结尾，返回 true，否则返回 false。任意参数为 NULL 则返回 NULL。

## 语法

```Haskell
ENDS_WITH(str, suffix)
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

`suffix`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 BOOLEAN。

## 示例

```Plain Text
MySQL > select ends_with("Hello starrocks", "starrocks");
+-----------------------------------+
| ends_with('Hello starrocks', 'starrocks') |
+-----------------------------------+
|                                 1 |
+-----------------------------------+

MySQL > select ends_with("Hello starrocks", "Hello");
+-----------------------------------+
| ends_with('Hello starrocks', 'Hello') |
+-----------------------------------+
|                                 0 |
+-----------------------------------+
```
