---
displayed_sidebar: docs
---

# time_format



按指定格式格式化 TIME 类型时间值。

## 语法

```Haskell
VARCHAR TIME_FORMAT(TIME time, VARCHAR format)
```

## 参数说明

- `time`（必须）：待格式化的 TIME 类型时间值。
- `format`（必须）：要使用的输出格式。有效值：

```Plain Text
%f	Microseconds (000000 至 999999)
%H	Hour (00 至 23)
%h	Hour (00 至 12)
%i	Minutes (00 至 59)
%p	AM or PM
%S	Seconds (00 至 59)
%s	Seconds (00 至 59)
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
