---
displayed_sidebar: docs
---

# date

日付または日時式の日付部分を抽出します。

## Syntax

```Haskell
DATE date(DATETIME|DATE expr)
```

## Parameters

`expr`: 日付または日時式。

## Return value

DATE 型の値を返します。入力が NULL または無効な場合は NULL が返されます。

## Examples

Example 1: 日時値の日付部分を抽出します。

```plaintext
SELECT DATE("2017-12-31 11:20:59");
+-----------------------------+
| date('2017-12-31 11:20:59') |
+-----------------------------+
| 2017-12-31                  |
+-----------------------------+
1 row in set (0.05 sec)
```

Example 2: 日付値の日付部分を抽出します。

```plaintext
SELECT DATE('2017-12-31');
+--------------------+
| date('2017-12-31') |
+--------------------+
| 2017-12-31         |
+--------------------+
1 row in set (0.08 sec)
```

Example 3: 現在のタイムスタンプの日付部分を抽出します。

```plaintext
SELECT DATE(current_timestamp());
+---------------------------+
| date(current_timestamp()) |
+---------------------------+
| 2022-11-08                |
+---------------------------+
1 row in set (0.05 sec)
```