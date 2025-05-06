---
displayed_sidebar: docs
---

# timestamp

## 説明

日付または日時式のDATETIME値を返します。

## 構文

```Haskell
DATETIME timestamp(DATETIME|DATE expr);
```

## パラメータ

`expr`: 変換したい時間式です。DATETIMEまたはDATE型である必要があります。

## 戻り値

DATETIME値を返します。入力時間が空または存在しない場合（例: `2021-02-29`）、NULLが返されます。

## 例

```Plain Text
select timestamp("2019-05-27");
+-------------------------+
| timestamp('2019-05-27') |
+-------------------------+
| 2019-05-27 00:00:00     |
+-------------------------+
1 row in set (0.00 sec)
```