---
displayed_sidebar: "Chinese"
---

# concat_ws

## 功能

使用分隔符将两个或以上的字符串拼接成一个新的字符串。新字符串使用分隔符进行连接。

### 语法

```Haskell
VARCHAR concat_ws(VARCHAR sep, VARCHAR str,...)
```

### 参数说明

- `sep`: 分隔符，数据类型 VARCHAR。
- `str`: 待拼接的字符串，数据类型 VARCHAR。该函数不会跳过空字符串，会跳过 NULL 值。

### 返回值说明

返回 VARCHAR 类型的字符串。如果分隔符为 NULL，返回 NULL。

### 示例

示例1：使用 `r` 作为分隔符，返回 `starrocks`。

```Plain Text
MySQL > select concat_ws("r", "sta", "rocks");
+--------------------------------+
| concat_ws('r', 'sta', 'rocks') |
+--------------------------------+
| starrocks                      |
+--------------------------------+
```

示例2：使用 `NULL` 作为分隔符，返回 NULL。

```Plain Text
MySQL > select concat_ws(NULL, "star", "rocks");
+----------------------------------+
| concat_ws(NULL, 'star', 'rocks') |
+----------------------------------+
| NULL                             |
+----------------------------------+
```

示例3：使用 `r` 作为分隔符，跳过 NULL 值。

```Plain Text
MySQL > select concat_ws("r", "sta", NULL,"rocks");
+-------------------------------------+
| concat_ws("r", "sta", NULL,"rocks") |
+-------------------------------------+
| starrocks                           |
+-------------------------------------+
```
