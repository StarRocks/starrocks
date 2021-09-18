# Flink Connector常见问题

## Flink中localtimestap函数生成的时间，在Flink中时间正常，sink到Doirs后发现时间落后8小时。已确认Flink所在服务器与Doris所在服务器时区均为Asia/ShangHai东八区。Flink版本为1.12，驱动为flink-connector-jdbc_2.11，需要如何处理？

可以在Flink sink表中配置时区参数'server-time-zone' = 'Asia/Shanghai'，或同时在jdbc url里添加&serverTimezone=Asia/Shanghai。示例如下：

```sql
CREATE TABLE sk (
    sid int,
    local_dtm TIMESTAMP,
    curr_dtm TIMESTAMP
)
WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://192.168.110.66:9030/sys_device?characterEncoding=utf-8&serverTimezone=Asia/Shanghai',
    'table-name' = 'sink',
    'driver' = 'com.mysql.jdbc.Driver',
    'username' = 'doris',
    'password' = 'doris123',
    'server-time-zone' = 'Asia/Shanghai'
);
```
