---
displayed_sidebar: "Chinese"
---

# bit_shift_left

## 功能

将一个数值或数值表达式的二进制表示向左移动指定的位数。该函数执行**算术左移**。

算术左移时，依次左移一位，尾部补 0，左侧最高位保持不变，bit 长度不变。例如，`10101` 算术左移一位的结果是 `11010`。

## 语法

```Haskell
bit_shift_left(value, shift)
```

## 参数说明

`value`: 要进行位移的数值或数值表达式。支持如下数据类型：TINYINT、SMALLINT、INT、BIGINT、LARGEINT。

`shift`: 要移动的位数，BIGINT 类型。

## 返回值说明

返回值的数据类型和 `value` 一致。

## 使用说明

- 如果任何一个输入参数为 NULL，则返回 NULL。
- 如果 `shift` 小于 0，则返回 0。
- 对于任意 `valu``e` 值，如果 `shift` 等于 0，则返回原本的 `value` 值。
- 如果 `value` 等于 0，则固定返回 0。
- 如果 `value` 为非整型的数值，会转换为整数进行运算。参见[示例](#示例)。
- 如果 `value` 为 STRING 类型，会转换为整数进行运算。如果 STRING 无法转换为整数，则作为 NULL 处理。参见[示例](#示例)。

## 示例

执行算术左移。

```Plain
SELECT bit_shift_left(2, 1);
+----------------------+
| bit_shift_left(2, 1) |
+----------------------+
|                    4 |
+----------------------+

SELECT bit_shift_left(2.2, 1);
+------------------------+
| bit_shift_left(2.2, 1) |
+------------------------+
|                      4 |
+------------------------+

SELECT bit_shift_left("2", 1);
+------------------------+
| bit_shift_left('2', 1) |
+------------------------+
|                      4 |
+------------------------+

SELECT bit_shift_left(-2, 1);
+-----------------------+
| bit_shift_left(-2, 1) |
+-----------------------+
|                    -4 |
+-----------------------+
```

## 相关文档

- [bit_shift_right](bit_shift_right.md)
- [bit_shift_right_logical](bit_shift_right_logical.md)
