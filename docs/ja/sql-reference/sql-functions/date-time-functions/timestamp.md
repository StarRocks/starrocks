---
displayed_sidebar: docs
---

# timestamp

日付または日時式の DATETIME 値を返します。

## Syntax

```Haskell
DATETIME timestamp(DATETIME|DATE expr);
```

## Parameters

`expr`: 変換したい時間式。DATETIME または DATE 型である必要があります。

## Return value

DATETIME 値を返します。入力された時間が空または存在しない場合（例: `2021-02-29`）、NULL が返されます。

## Examples

```Plain Text
select timestamp("2019-05-27");
+-------------------------+
| timestamp('2019-05-27') |
+-------------------------+
| 2019-05-27 00:00:00     |
+-------------------------+
1 row in set (0.00 sec)
```