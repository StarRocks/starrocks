---
displayed_sidebar: docs
---

# sleep

## Description

指定された時間（秒単位）だけ操作の実行を遅延させ、遅延が中断されずに完了したかどうかを示す BOOLEAN 値を返します。中断されずに遅延が完了した場合は `1` が返されます。それ以外の場合は `0` が返されます。

## Syntax

```Haskell
BOOLEAN sleep(INT x);
```

## Parameters

`x`: 操作の実行を遅延させたい時間の長さを指定します。INT 型でなければなりません。単位は秒です。入力が NULL の場合、遅延せずに即座に NULL が返されます。

## Return value

BOOLEAN 型の値を返します。

## Examples

```Plain Text
select sleep(3);
+----------+
| sleep(3) |
+----------+
|        1 |
+----------+
1 row in set (3.00 sec)

select sleep(NULL);
+-------------+
| sleep(NULL) |
+-------------+
|        NULL |
+-------------+
1 row in set (0.00 sec)
```

## Keywords

SLEEP, sleep