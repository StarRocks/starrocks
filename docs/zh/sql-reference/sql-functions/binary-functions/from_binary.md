---
displayed_sidebar: docs
---

# from_binary

## 功能

根据指定的格式，将二进制数据转化为 VARCHAR 类型的字符串。支持的二进制格式包括 Hex, Base64，和 UTF-8。如果未指定，默认为 Hex。

该函数从 3.0 版本开始支持。

## Syntax

```Haskell
from_binary(binary[, binary_type])
```

## Parameters

- `binary`: 必选，待转换的二进制数值。
- `binary_type`: 可选，指定的二进制格式。有效值：`hex`，`encode64`，`utf8`。

  - `hex` (默认值): 该函数使用十六进制编码将二进制数值转换为 VARCHAR 字符串。
  - `encode64`: 该函数使用 Base64 编码将二进制数值转换为 VARCHAR 字符串。
  - `utf8`: 该函数直接将二进制数值转换为 VARCHAR 字符串，不做任何转换处理。

## Return value

根据指定的格式，返回 VARCHAR 字符串。

## Examples

```Plain
mysql> select from_binary(to_binary('ABAB', 'hex'), 'hex');
+----------------------------------------------+
| from_binary(to_binary('ABAB', 'hex'), 'hex') |
+----------------------------------------------+
| ABAB                                         |
+----------------------------------------------+
1 row in set (0.02 sec)

mysql> select from_base64(from_binary(to_binary('U1RBUlJPQ0tT', 'encode64'), 'encode64'));
+-----------------------------------------------------------------------------+
| from_base64(from_binary(to_binary('U1RBUlJPQ0tT', 'encode64'), 'encode64')) |
+-----------------------------------------------------------------------------+
| STARROCKS                                                                   |
+-----------------------------------------------------------------------------+
1 row in set (0.01 sec)

mysql> select from_binary(to_binary('STARROCKS', 'utf8'), 'utf8');
+-----------------------------------------------------+
| from_binary(to_binary('STARROCKS', 'utf8'), 'utf8') |
+-----------------------------------------------------+
| STARROCKS                                           |
+-----------------------------------------------------+
1 row in set (0.01 sec)
```

## References

- [to_binary](to_binary.md)
- [BINARY/VARBINARY](../../data-types/string-type/BINARY.md)
