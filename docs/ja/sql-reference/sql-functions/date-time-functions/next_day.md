---
displayed_sidebar: docs
---

# next_day

指定された曜日 (DOW) のうち、入力された日付 (DATE または DATETIME) の後に最初に来る日付を返します。例えば、`next_day('2023-04-06', 'Monday')` は '2023-04-06' の後に来る次の月曜日の日付を返します。

この関数は v3.1 からサポートされています。[previous_day](./previous_day.md) の反対です。

## Syntax

```SQL
DATE next_day(DATETIME|DATE date_expr, VARCHAR dow)
```

## Parameters

- `date_expr`: 入力日付。これは有効な DATE または DATETIME 式でなければなりません。
- `dow`: 曜日。大文字小文字を区別するいくつかの省略形が有効です。
  
  | DOW_FULL  | DOW_2 | DOW_3 |
  | --------- | ----- |:-----:|
  | Sunday    | Su    | Sun   |
  | Monday    | Mo    | Mon   |
  | Tuesday   | Tu    | Tue   |
  | Wednesday | We    | Wed   |
  | Thursday  | Th    | Thu   |
  | Friday    | Fr    | Fri   |
  | Saturday  | Sa    | Sat   |

## Return value

DATE 値を返します。

無効な `dow` はエラーを引き起こします。`dow` は大文字小文字を区別します。

無効な日付または NULL 引数が渡されると、NULL を返します。

## Examples

```Plain
-- 2023-04-06 の後に来る次の月曜日の日付を返します。2023-04-06 は木曜日で、次の月曜日の日付は 2023-04-10 です。

MySQL > select next_day('2023-04-06', 'Monday');
+----------------------------------+
| next_day('2023-04-06', 'Monday') |
+----------------------------------+
| 2023-04-10                       |
+----------------------------------+

MySQL > select next_day('2023-04-06', 'Tue');
+-------------------------------+
| next_day('2023-04-06', 'Tue') |
+-------------------------------+
| 2023-04-11                    |
+-------------------------------+

MySQL > select next_day('2023-04-06 20:13:14', 'Fr');
+---------------------------------------+
| next_day('2023-04-06 20:13:14', 'Fr') |
+---------------------------------------+
| 2023-04-07                            |
+---------------------------------------+
```

## keyword

NEXT_DAY, NEXT