
# stddev, stddev_pop

## 功能

返回 `expr` 表达式的总体标准差。

### 语法

```Haskell
STDDEV(expr)
```

## 参数说明

`epxr`: 被选取的表达式。

## 返回值说明

返回值为数值类型。

## 示例

```plain text
mysql> SELECT  stddev(lo_quantity), stddev_pop(lo_quantity) from lineorder;
+---------------------+-------------------------+
| stddev(lo_quantity) | stddev_pop(lo_quantity) |
+---------------------+-------------------------+
|   14.43100708360797 |       14.43100708360797 |
+---------------------+-------------------------+
```
