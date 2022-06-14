# concat_ws

## 功能

使用第一个参数 sep 作为连接符, 将第二个参数以及后续所有参数拼接成一个字符串, 若分隔符是 NULL, 则返回 NULL
> 注: `concat_ws` 函数不会跳过空字符串, 会跳过 NULL 值

### 语法

```Haskell
concat_ws(sep, str,...)
```

## 参数说明

`sep`: 支持的数据类型为 VARCHAR

`str`: 支持的数据类型为 VARCHAR

## 返回值说明

返回值的数据类型为 VARCHAR

## 示例

```Plain Text
MySQL > select concat_ws("r", "sta", "rocks");
+--------------------------------+
| concat_ws('r', 'sta', 'rocks') |
+--------------------------------+
| starrocks                      |
+--------------------------------+

MySQL > select concat_ws(NULL, "star", "rocks");
+----------------------------------+
| concat_ws(NULL, 'star', 'rocks') |
+----------------------------------+
| NULL                             |
+----------------------------------+

MySQL > select concat_ws("r", "sta", NULL,"rocks");
+-------------------------------------+
| concat_ws("r", "sta", NULL,"rocks") |
+-------------------------------------+
| starrocks                           |
+-------------------------------------+
```
