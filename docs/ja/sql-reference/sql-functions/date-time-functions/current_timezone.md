---
displayed_sidebar: docs
description: "現在の日付を取得し、DATETIME 型の値を返します。"
---

# current_timestamp

現在のタイムゾーンを取得し、VARCHAR 値を返します。

## Syntax

```Haskell
VARCHAR CURRENT_TIMEZONE()
```

## Examples

```Plain Text
MySQL > select current_timezone();
+---------------------+
| current_timezone()  |
+---------------------+
| America/Los_Angeles |
+---------------------+
```
