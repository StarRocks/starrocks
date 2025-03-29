---
displayed_sidebar: docs
---

# time_format

指定されたフォーマットで TIME 型の値をフォーマットします。

## Syntax

```Haskell
VARCHAR TIME_FORMAT(TIME time, VARCHAR format)
```

## Parameters

- `time` (必須): フォーマットする TIME 型の時間値。
- `format` (必須): 使用するフォーマット。使用可能な値:

```Plain Text
%f	マイクロ秒 (000000 から 999999)
%H	時間 (00 から 23)
%h	時間 (00 から 12)
%i	分 (00 から 59)
%p	AM または PM
%S	秒 (00 から 59)
%s	秒 (00 から 59)
```

## Examples

```Plain Text
mysql> SELECT TIME_FORMAT("19:30:10", "%h %i %s %p");
+----------------------------------------+
| time_format('19:30:10', '%h %i %s %p') |
+----------------------------------------+
| 12 00 00 AM                            |
+----------------------------------------+
1 row in set (0.01 sec)

```