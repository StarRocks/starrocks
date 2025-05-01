---
displayed_sidebar: docs
---

# timestampadd

## 説明

整数式の間隔を日付または日時式 `datetime_expr` に追加します。

間隔の単位は以下のいずれかでなければなりません：

MILLISECOND (3.2以降)、SECOND、MINUTE、HOUR、DAY、WEEK、MONTH、または YEAR。

## 構文

```Haskell
DATETIME TIMESTAMPADD(unit, interval, DATETIME datetime_expr)
```

## パラメータ

- `datetime_expr`: 時間間隔を追加したい DATE または DATETIME の値。
- `interval`: 追加する間隔の数を指定する整数式。
- `unit`: 追加する時間間隔の単位。サポートされている単位には MILLISECOND (3.2以降)、SECOND、MINUTE、HOUR、DAY、WEEK、MONTH、YEAR が含まれます。

## 戻り値

`datetime_expr` と同じデータ型の値を返します。

## 例

```plain text

MySQL > SELECT TIMESTAMPADD(MINUTE,1,'2019-01-02');
+------------------------------------------------+
| timestampadd(MINUTE, 1, '2019-01-02 00:00:00') |
+------------------------------------------------+
| 2019-01-02 00:01:00                            |
+------------------------------------------------+

MySQL > SELECT TIMESTAMPADD(WEEK,1,'2019-01-02');
+----------------------------------------------+
| timestampadd(WEEK, 1, '2019-01-02 00:00:00') |
+----------------------------------------------+
| 2019-01-09 00:00:00                          |
+----------------------------------------------+

MySQL > SELECT TIMESTAMPADD(MILLISECOND,1,'2019-01-02');
+--------------------------------------------+
| timestampadd(MILLISECOND, 1, '2019-01-02') |
+--------------------------------------------+
| 2019-01-02 00:00:00.001000                 |
+--------------------------------------------+
```

## キーワード

TIMESTAMPADD