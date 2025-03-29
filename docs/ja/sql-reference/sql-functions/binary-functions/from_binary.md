---
displayed_sidebar: docs
---

# from_binary

指定されたバイナリ形式 (`binary_type`) に基づいて、バイナリ値を VARCHAR 文字列に変換します。サポートされているバイナリ形式は、`hex`、`encode64`、および `utf8` です。`binary_type` が指定されていない場合、デフォルトは `hex` です。

## Syntax

```Haskell
from_binary(binary[, binary_type])
```

## Parameters

- `binary`: 変換する入力バイナリ。必須です。

- `binary_type`: 変換のためのバイナリ形式。オプションです。

  - `hex` (デフォルト): `from_binary` は `hex` メソッドを使用して入力バイナリを VARCHAR 文字列にエンコードします。
  - `encode64`: `from_binary` は `base64` メソッドを使用して入力バイナリを VARCHAR 文字列にエンコードします。
  - `utf8`: `from_binary` は入力バイナリを変換せずに VARCHAR 文字列にします。

## Return value

VARCHAR 文字列を返します。

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
- [BINARY/VARBINARY data type](../../data-types/string-type/BINARY.md)