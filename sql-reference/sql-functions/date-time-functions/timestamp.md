# timestamp

## 功能

将参数 `expr` 转为时间戳形式作为结果返回。

## 语法

```Haskell
DATETIME timestamp(DATETIME expr);
```

## 参数说明

`expr`: 支持的数据类型为 DATETIME。

## 返回值说明

返回值的数据类型为 DATETIME。

## 示例

```Plain Text
select timestamp("2019-05-27");
+-------------------------+
| timestamp('2019-05-27') |
+-------------------------+
| 2019-05-27 00:00:00     |
+-------------------------+
1 row in set (0.00 sec)
```
