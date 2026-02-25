---
displayed_sidebar: docs
---

# str2date

指定されたフォーマットに従って、文字列を DATE 値に変換します。変換に失敗した場合は、NULL が返されます。

フォーマットは [date_format](./date_format.md) で説明されているものと一致している必要があります。

この関数は [str_to_date](../date-time-functions/str_to_date.md) と同等ですが、異なる戻り値の型を持ちます。

## 構文

```Haskell
DATE str2date(VARCHAR str, VARCHAR format);
```

## パラメータ

`str`: 変換したい時間表現。VARCHAR 型である必要があります。

`format`: 値を返すために使用されるフォーマット。サポートされているフォーマットについては、[date_format](./date_format.md) を参照してください。

## 戻り値

DATE 型の値を返します。

`str` または `format` が NULL の場合は、NULL が返されます。

## 例

```Plain
select str2date('2010-11-30 23:59:59', '%Y-%m-%d %H:%i:%s');
+------------------------------------------------------+
| str2date('2010-11-30 23:59:59', '%Y-%m-%d %H:%i:%s') |
+------------------------------------------------------+
| 2010-11-30                                           |
+------------------------------------------------------+
1 row in set (0.01 sec)
```