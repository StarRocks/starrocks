---
displayed_sidebar: docs
---

# from_unixtime_ms

## 説明

ミリ秒単位のUnixタイムスタンプを日時文字列に変換します。現在、'yyyy-MM-dd HH:mm:ss'形式のみをサポートしています。

この関数は[from_unixtime](./from_unixtime.md)に似ていますが、秒単位ではなくミリ秒単位のUnixタイムスタンプを受け入れます。

対応する`unix_timestamp_ms()`関数がないことに注意してください。

この関数はタイムゾーン設定の影響を受けます。詳細については、[タイムゾーンの設定](../../../administration/management/timezone.md)を参照してください。

## 構文

```Haskell
VARCHAR from_unixtime_ms(BIGINT unix_timestamp_ms)
```

## パラメータ

- `unix_timestamp_ms`: 変換するミリ秒単位のUnixタイムスタンプ（BIGINT型）。タイムスタンプが0未満または2147483647000より大きい場合、NULLが返されます。有効なタイムスタンプ範囲は1970-01-01 00:00:00.000から2038-01-19 11:14:07.000までです。

## 戻り値

VARCHAR型の日時文字列を返します。

入力タイムスタンプが範囲外の場合、NULLが返されます。

## 例

```Plain Text
mysql> SELECT from_unixtime_ms(1196440219000);
+------------------------------+
| from_unixtime_ms(1196440219000) |
+------------------------------+
| 2007-12-01 00:30:19          |
+------------------------------+

mysql> SELECT from_unixtime_ms(1196440219123);
+------------------------------+
| from_unixtime_ms(1196440219123) |
+------------------------------+
| 2007-12-01 00:30:19          |
+------------------------------+

```

## キーワード

FROM_UNIXTIME_MS, FROM, UNIXTIME, MS, MILLISECONDS

## 関連機能

- [from_unixtime](./from_unixtime.md): 秒単位のUnixタイムスタンプを日時文字列に変換します