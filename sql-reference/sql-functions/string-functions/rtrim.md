# rtrim

## 功能

将参数 `str` 从结尾部分开始连续出现的空格去掉。

## 语法

```Haskell
rtrim(str);
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

将字符串 `   ab d   ` 后面的空格去掉。

```Plain Text
mysql> SELECT rtrim('   ab d   ');
+---------------------+
| rtrim('   ab d   ') |
+---------------------+
|    ab d             |
+---------------------+
1 row in set (0.00 sec)
```
