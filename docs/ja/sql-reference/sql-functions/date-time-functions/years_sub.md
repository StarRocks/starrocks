---
displayed_sidebar: docs
---

# years_sub

指定された年数を、指定された日時または日付から引きます。

## Syntax

```Haskell
DATETIME YEARS_SUB(DATETIME date, INT years)
```

## Parameters

`date`: 元の日付時刻。DATETIME または DATE 型。

`years`: 引く年数。この値は負の値も可能ですが、date の年から years を引いた結果が 10000 を超えてはいけません。例えば、date の年が 2022 の場合、years は -7979 より小さくできません。同時に、years は date の年の値を超えてはいけません。例えば、date の年の値が 2022 の場合、years は 2022 より大きくできません。

## Return value

戻り値の型はパラメータ `date` と同じです。結果の年が範囲 [0, 9999] を超える場合は NULL を返します。

## Examples

```Plain Text
select years_sub("2022-12-20 15:50:21", 2);
+-------------------------------------+
| years_sub('2022-12-20 15:50:21', 2) |
+-------------------------------------+
| 2020-12-20 15:50:21                 |
+-------------------------------------+
```
