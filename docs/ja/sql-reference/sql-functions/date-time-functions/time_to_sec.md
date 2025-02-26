---
displayed_sidebar: docs
---

# time_to_sec

時間の値を秒数に変換します。変換に使用される式は次の通りです：

Hour x 3600 + Minute x 60 + Second

## Syntax

```Haskell
BIGINT time_to_sec(TIME time)
```

## Parameters

`time`: TIME 型である必要があります。

## Return value

BIGINT 型の値を返します。入力が無効な場合は、NULL が返されます。

## Examples

```plain text
select time_to_sec('12:13:14');
+-----------------------------+
| time_to_sec('12:13:14')     |
+-----------------------------+
|                        43994|
+-----------------------------+
```