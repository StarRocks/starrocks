---
displayed_sidebar: docs
---

# bit_shift_left

数値式の2進数表現を指定されたビット数だけ左にシフトします。

この関数は**算術左シフト**を実行します。この操作ではビット長は変わらず、末尾に0が追加され、最上位ビットは変わりません。例えば、`10101` を1ビット左にシフトすると `11010` になります。

## Syntax

```Haskell
bit_shift_left(value, shift)
```

## Parameters

`value`: シフトする値または数値式。サポートされているデータ型は TINYINT、SMALLINT、INT、BIGINT、および LARGEINT です。

`shift`: シフトするビット数。サポートされているデータ型は BIGINT です。

## Return value

`value` と同じ型の値を返します。

## Usage notes

- 任意の入力パラメータが NULL の場合、NULL が返されます。
- `shift` が 0 未満の場合、0 が返されます。
- `value` を `0` でシフトすると、常に元の `value` になります。
- `0` を `shift` でシフトすると、常に `0` になります。
- `value` のデータ型が数値であっても整数でない場合、その値は整数にキャストされます。[Examples](#examples) を参照してください。
- `value` のデータ型が文字列の場合、可能であればその値は整数にキャストされます。値が整数にキャストできない場合、NULL として処理されます。[Examples](#examples) を参照してください。

## Examples

この関数を使用して数値をシフトします。

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

## References

- [bit_shift_right](bit_shift_right.md)
- [bit_shift_right_logical](bit_shift_right_logical.md)