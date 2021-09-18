# Insert Into常见问题

## 进行数据insert，SQL每插入一条大约耗时50~100ms之间，执行效率有没有什么可以优化的？

OLAP不建议使用insert单条写入，都是批量写入的。单条写入和批量写入，时间是一样的。

## insert into select 的时候报错index channel has intoleralbe failure

因为导入过程中存在了配置参数的超时判定，修改了流式导入RPC的超时时间即可解决。fe.conf，be.conf 配置中将下面这两个项改大些 (也可以在manager页面进项修改）

```plain text
streaming_load_rpc_max_alive_time_sec=2400
tablet_writer_open_rpc_timeout_sec=120
```
