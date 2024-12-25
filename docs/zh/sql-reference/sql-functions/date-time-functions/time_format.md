---
displayed_sidebar: docs
---

# time_format



TIME_FORMAT() 函数按指定格式格式化时间。

* `date` 参数是合法的日期。
* `format` 指定日期/时间的输出格式。

可以使用的格式有：

```Plain Text
%f	Microseconds (000000 to 999999)
%H	Hour (00 to 23)
%h	Hour (00 to 12)
%i	Minutes (00 to 59)
%p	AM or PM
%S	Seconds (00 to 59)
%s	Seconds (00 to 59)
```

## 语法

```Haskell
VARCHAR TIME_FORMAT(TIME time, VARCHAR format)
```

## 示例

```Plain Text
mysql> SELECT TIME_FORMAT("19:30:10", "%h %i %s %p");
+----------------------------------------+
| time_format('19:30:10', '%h %i %s %p') |
+----------------------------------------+
| 12 00 00 AM                            |
+----------------------------------------+
1 row in set (0.01 sec)

```
