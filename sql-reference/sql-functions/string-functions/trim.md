# trim

## 功能

将参数 `expr` 中从左侧和右侧开始部分连续出现的空格去掉。

## 语法

```Haskell
trim(expr);
```

## 参数说明

`expr`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

去除字符 a 左侧和 c 右侧共 5 个空格。

```Plain Text
MySQL > SELECT trim("   ab c  ");
+-------------------+
| trim('   ab c  ') |
+-------------------+
| ab c              |
+-------------------+
1 row in set (0.00 sec)
```
