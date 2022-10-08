# time_to_sec

## Description

函数返回将参数time 转换为秒数的时间值，转换公式为  小时*3600 + 分钟*60+ 秒

## Syntax

```Haskell
INT time_to_sec(DATETIME date)
```

## Example

```plain text
select time_to_sec('12:13:14');
+-----------------------------+
| time_to_sec('12:13:14')     |
+-----------------------------+
|                        43994|
+-----------------------------+
```

## keyword

`TIME_TO_SEC`
