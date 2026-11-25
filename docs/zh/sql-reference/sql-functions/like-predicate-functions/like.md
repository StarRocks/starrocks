---
displayed_sidebar: docs
description: "检查字符串是否模糊匹配给定的模式。"
products: [oss, enterprise, cloud, anywhere]
---

# like



判断字符串 `expr` 是否**模糊匹配**给定的模式 `pattern`，匹配成功返回 1，否则返回 0。LIKE 通常与 `%`、`_` 结合使用，`%` 表示 0 个、一个或多个字符，`_` 表示单个字符。

## 语法

```Haskell
BOOLEAN like(VARCHAR expr, VARCHAR pattern);
```

## 参数说明

`expr`: 目标字符串，支持的数据类型为 VARCHAR。

`pattern`: 字符串需匹配的模式，支持的数据类型为 VARCHAR。

## 转义字符

如需匹配字面量 `%` 或 `_`，请在其前面加上转义字符。默认的转义字符是反斜杠，在字符串字面量中写作 `\\`。

将 LIKE 用作运算符时，可以通过 ESCAPE 子句为模式指定其他转义字符：

```SQL
expr [NOT] LIKE pattern ESCAPE 'escape_char'
```

`escape_char` 为单个字符；如果模式没有转义字符，则为空字符串。使用 ESCAPE 子句时，`pattern` 必须是常量表达式。

```SQL
-- 返回 c1 以 "50%" 结尾的行。
SELECT * FROM t1 WHERE c1 LIKE '%50^%' ESCAPE '^';
```

## 返回值说明

返回值的数据类型为 BOOLEAN。

## 示例

```Plain Text

mysql> select like("star","star");
+----------------------+
| like('star', 'star') |
+----------------------+
|                    1 |
+----------------------+


mysql> select like("starrocks","star%");
+----------------------+
| like('star', 'star') |
+----------------------+
|                    1 |
+----------------------+
1 row in set (0.00 sec)
```
