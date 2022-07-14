# Configure a time zone

This topic describes how to configure a time zone.

## Configure a session-level time zone or a global time zone

You can configure a time zone by using the following parameter:

`time_zone`: the time zone of a frontend (FE). You can specify a session-level time zone or a global time zone:

- To specify a session-level time zone, execute the command `SET time_zone = 'xxx'`. You can set different time zones for different sessions. The time zone setting becomes invalid if you disconnect with the FE.
- To specify a global time zone, execute the command `SET global time_zone = 'xxx'`. When you specify a global time zone for an FE, the time zone setting is effective for all the FEs within your cluster. The time zone setting is persisted in FEs. The time zone setting is still valid if you disconnect with FEs.

> Note: Before you load data into StarRocks, modify the value of the global `time_zone` parameter for an FE to the same value of the `system_time_zone` parameter for the machine. Otherwise, after data loading, the values of the fields of the DATE data type are incorrect. The `system_time_zone` parameter refers to the time zone of the machine that is used to install an FE. When the machine is started, the time zone of the machine is recorded as the value of this parameter. You cannot manually configure this parameter.

### Time zone format

The value of the `time_zone` parameter is not case-sensitive. The value of the parameter can be in one of the following formats.

| **Format**     | **Example**                                                  |
| -------------- | ------------------------------------------------------------ |
| UTC offset     | `SET time_zone = '+10:00'` `SET global time_zone = '-6:00'` |
| Time zone name | `SET time_zone = 'Asia/Shanghai'` `SET global time_zone = 'America/Los-Angelas'` |

For more information, see [List of tz database time zones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones).

> Note: Time zone abbreviations are not supported except for CST. If you set the value of `time_zone` as `CST`, StarRocks converts `CST` into `Asia/Shanghai`.

### Default time zone

The default value of the `time_zone` parameter is `Asia/Shanghai`.

## View time zone settings

To view the time zone setting, execute the command: `SHOW variables like '%time_zone%'`.

## Impacts of time zone settings

- Time zone settings affect the time values that are returned by the SHOW LOAD and SHOW BACKENDS statement. However, the settings do not affect the value of `LESS THAN` of the partitioning columns (in DATE or DATETIME data type) specified in CREATE TABLE statement and the display of the values stored as the DATE or DATETIME data types.
- The setting of time zone affects the display and storage of the following functions:
  - `FROM_UNIXTIME`: returns a date and time of your specified time zone based on a specified UTC timestamp. For example, if the value of the `time_zone` parameter is `Asia/Shanghai`, `FROM_UNIXTIME(0)` returns `1970-01-01 08:00:00`.
  - `UNIX_TIMESTAMP`: returns a UTC timestamp based on the date and time of your specified time zone. For example, if the value of the `time_zone` parameter is `Asia/Shanghai`, `UNIX_TIMESTAMP('1970-01-01 08:00:00')` returns `0`.
  - `CURTIME`: returns the current time of your specified time zone. For example, if the current time of a specified time zone is 16:34:05. `CURTIME()` returns `16:34:05`.
  - `NOW`: returns the current date and time of your specified time zone. For example, if the current date and time of a specified time zone is 2021-02-11 16:34:13, `NOW()` returns `2021-02-11 16:34:13`.
  - `CONVERT_TZ`: convert the date and time from one time zone to another. For example, `CONVERT_TZ('2021-08-01 11:11:11', 'Asia/Shanghai', 'America/Los_Angeles');` returns `2021-07-31 20:11:11`.
  