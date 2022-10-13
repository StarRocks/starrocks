# 设置时区

本文介绍了如何设置时区以及时区设置的影响。

## 设置会话/全局时区

您可以通过 `time_zone` 参数设置 StarRocks 时区，并指定其生效范围是会话级还是全局。

- 如指定会话级时区，执行 `SET time_zone = 'xxx';`。不同会话可以指定不同的时区，如断开和 FE 的连接，时区设置将会失效。
- 如指定全局时区，执行 `SET global time_zone = 'xxx';`。FE 会将该时区设置持久化，与 FE 连接断开后该设置仍有效。

> **说明**
>
> 在导入数据前，需确保 StarRocks 全局时区和部署 FE 机器的时区是一致的。否则导入后 DATE 类型数据会异常。`system_time_zone` 参数即表示部署 FE 机器的时区。机器启动时，机器的时区会被自动设为该参数的值且不能手动修改。

### 时区格式

时区值不区分大小写，支持以下格式：

| **格式**     | **示例**                                                     |
| ------------ | ------------------------------------------------------------ |
| UTC偏移量    | `SET time_zone = '+10:00';` `SET global time_zone = '-6:00';` |
| 标准时区格式 | `SET time_zone = 'Asia/Shanghai';` `SET global time_zone = 'America/Los_Angeles';` |

更多有关时区值格式说明，参见 [List of tz database time zones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)。

> **说明**
>
> 缩写时区格式仅支持 CST，StarRocks 会将 `CST` 转为标准时区 `Asia/Shanghai`。

### 默认时区

`time_zone` 参数的默认值为 `Asia/Shanghai`。

## 查看时区设置

如要查看时区设置，执行以下命令。

```Plain%20Text
 SHOW variables like '%time_zone%';
```

## 时区设置的影响

- 时区设置会影响 SHOW LOAD 和 SHOW BACKENDS 语句返回的时间值，但并不影响 CREATE TABLE 语句中分区列为 DATE 或 DATETIME 类型时 `LESS THAN` 字句指定的值，以及 DATE 或 DATETIME 类型的数据。
- 受时区设置影响的函数包括：
  - **from_unixtime**：给定一个 UTC 时间戳，返回指定时区的日期时间。如全局时区为 `Asia/Shanghai`，`select FROM_UNIXTIME(0);` 返回 `1970-01-01 08:00:00`。
  - **unix_timestamp**：给定一个指定时区的日期时间，返回 UTC 时间戳。如全局时区为 `Asia/Shanghai`，`select UNIX_TIMESTAMP('1970-01-01 08:00:00');` 返回 `0`。
  - **curtime**：返回指定时区的当前时间。如某时区当前时间为 16:34:05，`select CURTIME();` 返回 `16:34:05`。
  - **now**：返回指定时区的当前日期和时间。如某时区的当前日期和时间为 2021-02-11 16:34:13，`select NOW();` 返回 `2021-02-11 16:34:13`。
  - **convert_tz**：将一个指定时区的日期和时间转换到另一个指定时区。如指定 `select CONVERT_TZ('2021-08-01 11:11:11', 'Asia/Shanghai', 'America/Los_Angeles');`会返回 `2021-07-31 20:11:11`。
