---
displayed_sidebar: docs
---

# bit_shift_right

## Description

数値式の2進表現を指定されたビット数だけ右にシフトします。

この関数は**算術右シフト**を行い、ビット長は変わらず、下位ビットが削除され、符号ビットが上位ビットとして使用されます。例えば、`10101` を1ビット右にシフトすると `11010` になります。

## Syntax

```Haskell
bit_shift_right(value, shift)
```

## Parameters

`value`: シフトする値または数値式。サポートされているデータ型は TINYINT、SMALLINT、INT、BIGINT、および LARGEINT です。

`shift`: シフトするビット数。サポートされているデータ型は BIGINT です。

## Return value

`value` と同じ型の値を返します。

## Usage notes

- いずれかの入力パラメータが NULL の場合、NULL が返されます。
- `shift` が 0 未満の場合、0 が返されます。
- `value` を `0` でシフトすると、常に元の `value` になります。
- `0` を `shift` でシフトすると、常に `0` になります。
- `value` のデータ型が数値で整数でない場合、その値は整数にキャストされます。[Examples](#examples) を参照してください。
- `value` のデータ型が文字列の場合、その値は可能であれば整数にキャストされます。例えば、文字列 "2.3" は 2 にキャストされます。値が整数にキャストできない場合、その値は NULL として扱われます。[Examples](#examples) を参照してください。

## Examples

この関数を使用して数値をシフトします。

```Plain
SELECT bit_shift_right(2, 1);
+-----------------------+
| bit_shift_right(2, 1) |
+-----------------------+
|                     1 |
+-----------------------+

SELECT bit_shift_right(2.2, 1);
+-------------------------+
| bit_shift_right(2.2, 1) |
+-------------------------+
|                       1 |
+-------------------------+

SELECT bit_shift_right("2", 1);
+-------------------------+
| bit_shift_right('2', 1) |
+-------------------------+
|                       1 |
+-------------------------+

SELECT bit_shift_right(-2, 1);
+------------------------+
| bit_shift_right(-2, 1) |
+------------------------+
|                     -1 |
+------------------------+
```

## References

- [bit_shift_left](bit_shift_left.md)

- [bit_shift_right_logical](bit_shift_right_logical.md)