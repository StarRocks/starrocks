---
displayed_sidebar: docs
---

# bit_shift_right_logical

## 功能

将一个数值或者数值表达式的二进制表示向右移动指定的位数。该函数执行**逻辑右移**。

逻辑右移时，最低位丢弃，无论原始数值是正数还是负数均在最高位补 0，bit 长度不变。逻辑位移都是无符号位移。举例，`10101` 逻辑右移一位的结果是 `00101`.

bit_shift_right() 和 bit_shift_right_logical() 函数对于正数会返回相同结果。

## 语法

```Haskell
bit_shift_right_logical(value, shift)
```

## 参数说明

`value`: 要进行位移的数值或数值表达式。支持如下数据类型：TINYINT、SMALLINT、INT、BIGINT、LARGEINT。

`shift`: 要移动的位数，BIGINT 类型。

## 返回值说明

返回值的数据类型和 `value` 一致。

## 使用说明

- 如果任何一个输入参数为 NULL，则返回 NULL。
- 如果 `shift` 小于 0，则返回 0。
- 对于任意 `value` 值，如果 `shift` 等于 0，则返回原本的 `value` 值。
- 如果 `value` 等于 0，则固定返回 0。
- 如果 `value` 为非整型的数值，会转换为整数进行运算。参见[示例](#示例)。
- 如果 `value` 为 STRING 类型，会转换为整数进行运算。如果 STRING 无法转换为整数，则作为 NULL 处理。参见[示例](#示例)。

## 示例

执行逻辑右移。

```Plain
SELECT bit_shift_right_logical(2, 1);
+-------------------------------+
| bit_shift_right_logical(2, 1) |
+-------------------------------+
|                             1 |
+-------------------------------+

SELECT bit_shift_right_logical(2.2, 1);
+---------------------------------+
| bit_shift_right_logical(2.2, 1) |
+---------------------------------+
|                               1 |
+---------------------------------+

SELECT bit_shift_right_logical("2", 1);
+---------------------------------+
| bit_shift_right_logical('2', 1) |
+---------------------------------+
|                               1 |
+---------------------------------+

SELECT bit_shift_right_logical(cast('-2' AS INTEGER(32)), 1);
+-----------------------------------------------+
| bit_shift_right_logical(CAST('-2' AS INT), 1) |
+-----------------------------------------------+
|                                    2147483647 |
+-----------------------------------------------+
```

## 相关参考

- [bit_shift_left](bit_shift_left.md)
- [bit_shift_right](bit_shift_right.md)
