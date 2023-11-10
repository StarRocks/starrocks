---
displayed_sidebar: "Chinese"
---

# INSERT INTO 导入常见问题

本页列举了使用 INSERT INTO 语句导入数据时可能会遇到的常见问题及潜在解决方案。

## 使用 INSERT INTO 语句导入数据时，SQL 每插入一条大约耗时 50~100ms 之间，能否优化执行效率有？

因为 INSERT INTO 导入方式为批量写入，所以单条写入和批量写入的耗时相同。因此 OLAP 场景下不建议使用 INSERT INTO 语句单条写入数据。

## 使用 INSERT INTO SELECT 语句导入数据时，系统报错 “index channel has intoleralbe failure”。如何解决？

该错误因流式导入 RPC 超时导致。您可以通过在配置文件中调节 RPC 超时相关参数解决。

您需要在 BE 配置文件 **be.conf** 中修改以下系统配置项，并重启集群使修改生效：

`streaming_load_rpc_max_alive_time_sec`: 流式导入 RPC 的超时时间，默认为 1200，单位为秒。

您也可以通过设置以下系统变量调整查询的超时时间：

`query_timeout`：查询超时时间，单位为秒，默认为 `300`。

## 使用 INSERT INTO SELECT 语句导入大量数据时会执行失败 “execute timeout”。如何解决？

该错误因 query 超时导致。您可以通过调节 Session 变量 `query_timeout`解决。该参数默认为 300，单位为秒。

```sql
set query_timeout =xx;
```
