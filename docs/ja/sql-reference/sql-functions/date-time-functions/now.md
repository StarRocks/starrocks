---
displayed_sidebar: docs
---

# now, current_timestamp, localtime, localtimestamp

## 説明

現在の日付と時刻を返します。

この関数は、異なるタイムゾーンで異なる結果を返すことがあります。詳細については、 [Configure a time zone](../../../administration/timezone.md) を参照してください。

## 構文

```Haskell
DATETIME NOW()
```

## 例

```Plain Text
MySQL > select now();
+---------------------+
| now()               |
+---------------------+
| 2019-05-27 15:58:25 |
+---------------------+
```

## キーワード

NOW, now