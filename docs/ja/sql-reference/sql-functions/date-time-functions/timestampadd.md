---
displayed_sidebar: docs
---

# timestampadd

## 説明

整数式の間隔を日付または日時式 `datetime_expr` に追加します。

間隔の単位は以下のいずれかでなければなりません：

SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, または YEAR。

## 構文

```Haskell
DATETIME TIMESTAMPADD(unit, interval, DATETIME datetime_expr)
```

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
```

## キーワード

TIMESTAMPADD