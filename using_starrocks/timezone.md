# 设置时区

本文介绍了如何设置时区。

## 设置会话/全局时区

您可以通过以下参数设置时区：

`time_zone`: FE 时区。您可以指定会话级 (session) 时区或全局 (global) 时区。

- 如指定会话级时区，执行命令 `SET time_zone = 'xxx'`。不同会话可以指定不同的时区。如果您断开和FE的连接，时区设置将会失效。

- 如指定全局时区，执行命令 `SET global time_zone = 'xxx'`。FE会将该时区设置持久化，与 FE 连接断开后该设置仍有效。

> 说明：在导入数据前，需确保 FE 时区和部署 FE 的机器的时区是一致的。否则导入后表中 DATE 数据类型字段的值会异常。`system_time_zone` 参数即表示部署FE机器的时区。机器启动时，机器的时区会被自动设为该参数的值且不能手动修改。

### 时区格式

时区值不区分大小写，支持以下格式：

| **格式**     | **示例**                                                     |
| ------------ | ------------------------------------------------------------ |
| UTC偏移量    | `SET time_zone = '+10:00'` `SET global time_zone = '-6:00'`  |
| 标准时区格式 | `SET time_zone = 'Asia/Shanghai'` `SET global time_zone = 'America/Los_Angeles'` |

更多时区值的格式说明，参见 [List of tz database time zones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)。

> 说明：缩写时区格式仅支持 CST，StarRocks 会将 CST 转为标准时区 Asia/Shanghai。

### 默认时区

`time_zone` 参数的默认值为 `Asia/Shanghai`。

## 查看时区设置

如要查看时区设置，执行命令 `SHOW variables like '%time_zone%'`。

## 时区设置的影响

- 时区设置会影响 SHOW LOAD 和 SHOW BACKENDS 语句返回的时间值，但并不影响 CREATE TABLE 语句中数据类型为 DATE 或 DATETIME 分区列中的 `LESS THAN` 的值，以及存储为 DATE 或 DATETIME 数据类型的值的显示。

- 受时区设置影响的函数包括：
  - `FROM_UNIXTIME`：给定一个UTC时间戳，返回指定时区的日期时间。如 `time_zone` 参数的值为 `Asia/Shanghai`，`FROM_UNIXTIME(0)` 返回 `1970-01-01 08:00:00`。

- `UNIX_TIMESTAMP`：给定一个指定时区的日期时间，返回 UTC 时间戳。如 `time_zone` 参数的值为 `Asia/Shanghai`，`UNIX_TIMESTAMP('1970-01-01 08:00:00')` 返回 `0`。

- `CURTIME`：返回指定时区的当前时间。如某时区当前时间为 16:34:05，`CURTIME()` 返回 `16:34:05`。

- `NOW`：返回指定时区的当前日期和时间。如某时区的当前日期和时间为 2021-02-11 16:34:13，`NOW()` 返回 `2021-02-11 16:34:13`。

- `CONVERT_TZ`：将一个指定时区的日期和时间转换到另一个指定时区。如指定 `CONVERT_TZ('2021-08-01 11:11:11', 'Asia/Shanghai', 'America/Los_Angeles')` 会返回 `2021-07-31 20:11:11`。
