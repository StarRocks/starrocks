# 时区

## 什么是时区

StarRocks 支持多时区设置。StarRocks 支持如下两种时区相关参数。

* time_zone: 服务器当前时区，区分session级别和global级别。
* system_time_zone: 当服务器启动时，会根据机器设置时区自动设置，设置后不可修改。

## 如何设置时区

1. SHOW variables like '%time_zone%'

    查看当前时区相关配置

2. SET time_zone = 'Asia/Shanghai'

    该命令可以设置 session 级别的时区，连接断开后失效

3. SET global time_zone = 'Asia/Shanghai'

    该命令可以设置 global 级别的时区参数，fe会将参数持久化，连接断开后不失效

### 时区的值

时区值默认为 "Asia/Shanghai"，可以使用几种格式给出，不区分大小写，更多时区值的格式说明，请参见
[List of tz database time zones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)。

* 表示UTC偏移量的字符串，如 '+10:00' 或 '-6:00'。

* 标准时区格式，如 "Asia/Shanghai" 、 "America/Los_Angeles"。

* 不支持缩写时区格式，如 "MET" 、 "CTT" 。因为缩写时区在不同场景下存在歧义。

* 为了兼容 StarRocks ，支持 CST 缩写时区，内部会将 CST 转移为 "Asia/Shanghai" 的中国标准时区****

## 设置时区的影响

* 时区设置会影响对时区敏感的时间值的显示和存储。包括 NOW() 或 CURTIME() 等时间函数显示的值，也包括 show load, show backends 中的时间值。

  受时区影响的函数：

  * FROM_UNIXTIME：给定一个 UTC 时间戳，返回指定时区的日期时间：如 `FROM_UNIXTIME(0)`， 返回 CST 时区：`1970-01-01 08:00:00`。
  * UNIX_TIMESTAMP：给定一个指定时区日期时间，返回 UTC 时间戳：如 CST 时区 `UNIX_TIMESTAMP('1970-01-01 08:00:00')`，返回：`0`。
  * CURTIME：返回当前时区时间：如`CURTIME()`，返回：`16:34:05`。
  * NOW：返回当前时区日期时间：如`NOW()`，返回：`2021-02-11 16:34:13`。
  * CONVERT_TZ：将一个日期时间从一个指定时区转换到另一个指定时区。如`CONVERT_TZ('2021-08-01 11:11:11', 'Asia/Shanghai', 'America/Los_Angeles');`返回：`2021-07-31 20:11:11`。

* 不会影响 create table 中时间类型分区列的 less than 值，也不会影响存储为 date/datetime 类型的值的显示。

## 注意事项

系统的默认 time_zone 为 "Asia/Shanghai"，当导入时，如果服务器时区为其他时区，需要指定相应时区，否则日期字段会不一致。
例如系统时区为 UTC 时，未指定情况下导入结果的日期字段会出现 +8h 的异常结果，需要在导入的参数部分指定时区，具体参数指定参考对应 Load 章节的参数说明。
