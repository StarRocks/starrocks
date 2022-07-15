# Routine Load常见问题

## kafka 生产的mysql binlog数据算不算文本格式数据？

通过canal这种工具导入kafka以后是个json的格式。

## 从kafka消费数据再写入StarRocks，直连StarRocks，「语义一致性」能保证吗？

可以。

## recompile librdkafka with libsasl2 or openssl support

**问题描述：**

Routine load到 kafka集群失败报错，异常信息：

```plain text
ErrorReason{errCode = 4, msg='Job failed to fetch all current partition with error [Failed to send proxy request: failed to send proxy request: [PAUSE: failed to create kafka consumer: No provider for SASL mechanism GSSAPI: recompile librdkafka with libsasl2 or openssl support. Current build options: PLAIN SASL_SCRAM]]'}
```

**问题原因：**

当前librdkafka不支持sasl认证
