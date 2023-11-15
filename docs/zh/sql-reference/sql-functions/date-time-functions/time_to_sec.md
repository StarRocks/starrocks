# time_to_sec

## 功能

将 `time` 时间值转换为秒数，转换公式为:

`${小时}\times{3600} + {分钟}\times{60} + 秒$`

## 语法

```Haskell
BIGINT time_to_sec(TIME time)
```

## 参数说明

`time`：支持的数据类型为 TIME。

## 返回值说明

返回 BIGINT 类型的值。如果输入值格式非法，返回 NULL。

## 示例

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
