---
displayed_sidebar: docs
---

# quarter

日付の四半期部分を返します。範囲は1から4です。

## Syntax

```Haskell
INT quarter(DATETIME|DATE date);
```

## Parameters

`date`: DATETIME または DATE 型である必要があります。

## Return value

INT 値を返します。

以下のいずれかのシナリオでは NULL が返されます:

- 日付が有効な DATETIME または DATE 値でない場合。

- 入力が空の場合。

- 日付が存在しない場合、例えば 2022-02-29。

## Examples

Example 1: DATETIME 値の四半期部分を返します。

```Plain
SELECT QUARTER("2022-10-09 15:59:33");
+--------------------------------+
| quarter('2022-10-09 15:59:33') |
+--------------------------------+
|                              4 |
+--------------------------------+
```

Example 2: DATE 値の四半期部分を返します。

```Plain
SELECT QUARTER("2022-10-09");
+-----------------------+
| quarter('2022-10-09') |
+-----------------------+
|                     4 |
+-----------------------+
```

Example 3: 現在の時刻または日付に対応する四半期部分を返します。

```Plain
SELECT QUARTER(NOW());
+----------------+
| quarter(now()) |
+----------------+
|              4 |
+----------------+

SELECT QUARTER(CURDATE());
+--------------------+
| quarter(curdate()) |
+--------------------+
|                  4 |
+--------------------+
```