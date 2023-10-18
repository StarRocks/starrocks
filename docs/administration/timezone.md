# Configure a time zone

This topic describes how to configure a time zone and the impacts of time zone settings.

## Configure a session-level time zone or a global time zone

You can configure a session-level time zone or a global time zone for your StarRocks cluster using the `time_zone` parameter.

- To configure a session-level time zone, execute the command `SET time_zone = 'xxx';`. You can configure different time zones for different sessions. The time zone setting becomes invalid if you disconnect with FEs.
- To configure a global time zone, execute the command `SET global time_zone = 'xxx';`. The time zone setting is persisted in FEs and is valid even if you disconnect with FEs.

> **Note**
>
> Before you load data into StarRocks, modify the global time zone of your StarRocks cluster to the same value of the `system_time_zone` parameter. Otherwise, after data loading, data of the DATE type are incorrect. The `system_time_zone` parameter refers to the time zone of the machines that are used to host FEs. When the machines are started, the time zone of the machines is recorded as the value of this parameter. You cannot manually configure this parameter.

### Time zone format

The value of the `time_zone` parameter is not case-sensitive. The value of the parameter can be in one of the following formats.

| **Format**     | **Example**                                                  |
| -------------- | ------------------------------------------------------------ |
| UTC offset     | `SET time_zone = '+10:00';` `SET global time_zone = '-6:00';` |
| Time zone name | `SET time_zone = 'Asia/Shanghai';` `SET global time_zone = 'America/Los_Angeles';` |

For more information about time zone format, see [List of tz database time zones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones).

> **Note**
>
> Time zone abbreviations are not supported except for CST. If you set the value of `time_zone` to `CST`, StarRocks converts `CST` into `Asia/Shanghai`.

### Default time zone

The default value of the `time_zone` parameter is `Asia/Shanghai`.

## View time zone settings

To view the time zone setting, run the following command.

```plaintext
 SHOW VARIABLES LIKE '%time_zone%';
```

## Impacts of time zone settings

- Time zone settings affect the time values returned by the SHOW LOAD and SHOW BACKENDS statements. However, the settings do not affect the value specified in the `LESS THAN` clause when the partitioning columns specified in CREATE TABLE statement are of the DATE or DATETIME type. The settings also do not affect data of the DATE and DATETIME types.
- Time zone settings affect the display and storage of the following functions:
  - **from_unixtime**: returns a date and time of your specified time zone based on a specified UTC timestamp. For example, if the global time zone of your StarRocks cluster is `Asia/Shanghai`, `select FROM_UNIXTIME(0);` returns `1970-01-01 08:00:00`.
  - **unix_timestamp**: returns a UTC timestamp based on the date and time of your specified time zone. For example, if the global time zone of your StarRocks cluster is `Asia/Shanghai`, `select UNIX_TIMESTAMP('1970-01-01 08:00:00');` returns `0`.
  - **curtime**: returns the current time of your specified time zone. For example, if the current time of a specified time zone is 16:34:05. `select CURTIME();` returns `16:34:05`.
  - **now**: returns the current date and time of your specified time zone. For example, if the current date and time of a specified time zone is 2021-02-11 16:34:13, `select NOW();` returns `2021-02-11 16:34:13`.
  - **convert_tz**: converts the date and time from one time zone to another. For example, `select CONVERT_TZ('2021-08-01 11:11:11', 'Asia/Shanghai', 'America/Los_Angeles');` returns `2021-07-31 20:11:11`.
