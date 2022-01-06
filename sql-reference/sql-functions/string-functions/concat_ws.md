# concat_ws

## description

### Syntax

`VARCHAR concat_ws(VARCHAR sep, VARCHAR str,...)`

使用第一个参数 sep 作为连接符，将第二个参数以及后续所有参数拼接成一个字符串。
如果分隔符是 NULL，返回 NULL。
`concat_ws`函数不会跳过空字符串，会跳过 NULL 值

## example

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

## keyword

CONCAT_WS,CONCAT,WS
