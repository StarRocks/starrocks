# rtrim

## 功能

将参数 `expr` 中从右侧开始部分连续出现的空格去掉。

## 语法

```Haskell
rtrim(expr);
```

## 参数说明

`expr`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql> SELECT rtrim('   ab d   ');
+---------------------+
| rtrim('   ab d   ') |
+---------------------+
|    ab d             |
+---------------------+
1 row in set (0.00 sec)
```
