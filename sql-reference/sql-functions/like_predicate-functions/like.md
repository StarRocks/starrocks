# like

## 功能

对参数 `str` 进行模糊匹配，匹配成功返回 1，否则返回 0。通常与 `%`、`_` 结合使用，`%` 表示 0 个、一个或多个字符，`_` 表示单个字符。

## 语法

```Haskell
like(expr1,expr2);
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR。

`pattern`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 BOOLEAN。

## 示例

```Plain Text
mysql> select `like`("star","star");
+----------------------+
| like('star', 'star') |
+----------------------+
|                    1 |
+----------------------+
1 row in set (0.00 sec)
```
