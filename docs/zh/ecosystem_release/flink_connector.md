# Flink connector 版本发布

## 发布说明

**使用文档：**

- [使用 Flink connector 导入数据至 StarRocks](../loading/Flink-connector-starrocks.md)
- [使用 Flink connector 从 StarRocks 读取数据](../unloading/Flink_connector.md)

**源码下载地址：**[starrocks-connector-for-apache-flink](https://github.com/StarRocks/starrocks-connector-for-apache-flink)

**JAR 包命名规则：**

- Flink 1.15 及之后：`flink-connector-starrocks-${connector_version}_flink-${flink_version}.jar`
- Flink 1.15 之前：`flink-connector-starrocks-${connector_version}_flink-${flink_version}_${scala_version}.jar`

**JAR 包获取方式：**

您可以通过以下方式获取 Flink connector 的 JAR 包：

- 从 [Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks) 直接下载编译好的 JAR 包。
- 在 Maven 项目的 pom 文件添加 Flink connector 为依赖项，作为依赖下载。具体方式，参见[使用文档](../loading/Flink-connector-starrocks.md)。
- 使用源码手动编译成 JAR 包。具体方式，参见[使用文档](../loading/Flink-connector-starrocks.md)。

**版本要求：**

| Connector | Flink       | StarRocks  | Java | Scala      |
| --------- | ----------- | ---------- | ---- | ---------- |
| 1.2.8     | 1.13 ~ 1.17 | 2.1 及以上 | 8    | 2.11、2.12 |
| 1.2.7     | 1.11 ~ 1.15 | 2.1 及以上 | 8    | 2.11、2.12 |

> **注意**
>
> 最新版本的 Flink connector 只维护最近3个版本的 Flink。

## 发布记录

### 1.2

**1.2.8**

本版本发布包含如下功能优化和问题修复。其中重点优化如下：

- 支持 Flink 1.16 和 1.17。
- Sink 语义配置为 exactly-once 时建议设置 `sink.label-prefix`。使用说明，参考 [Exactly Once](../loading/Flink-connector-starrocks.md#exactly-once)。

**功能优化**

- 支持配置是否使用 Stream Load 事务接口来实现 at-least-once 语义。[#228](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/228)
- 为 sink 版本 V1 添加 retry 指标。[#229](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/229)
- 如果 EXISTING_JOB_STATUS 为 FINISHED，无需 getLabelState。[#231](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/231)
- 移除 sink 版本 V1 中无用的堆栈跟踪日志。[#232](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/232)
- [重构] 将 StarRocksSinkManagerV2 移动到 stream-load-sdk。[#233](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/233)
- 根据 Flink 中的表结构自动判断数据导入是否仅更新部分列，而不需要用户显式指定参数 `sink.properties.columns`。[#235](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/235)
- [重构] 将 probeTransactionStreamLoad 移动到 stream-load-sdk。 [#240](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/240)
- 为 stream-load-sdk 添加 git-commit-id-plugin。[#242](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/242)
- 在 info 级别的 log 中记录 DefaultStreamLoader#close。[#243](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/243)
- stream-load-sdk 支持生成不包含依赖的 jar。[#245](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/245)
- 在 stream-load-sdk 中使用 jackson 替换 fastjson。[#247](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/247)
- 支持处理 update_before 记录。[#250](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/250)
- 在文件中添加 Apache license。[#251](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/251)
- 支持获取 stream-load-sdk 返回的异常信息。[#252](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/252)
- 默认启用 `strip_outer_array` 和 `ignore_json_size`。[#259](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/259)
- 如果 sink 语义为 exactly-once，当 Flink job 恢复后，Flink connector 会尝试清理 StarRocks 中未包含在checkpoint 中的未完成事务。[#271](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/271)
- 重试失败后返回第一次的异常信息。[#279](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/279)

**问题修复**

- 修复 StarRocksStreamLoadVisitor 中的拼写错误。[#230](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/230)
- 修复 fastjson classloader 泄漏问题。[#260](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/260)

**测试**

- 增加从 Kafka 导入 StarRock 的测试框架。[#249](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/249)

**文档**

- 重构文档。[#262](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/262)
- 改进 sink 功能的文档。[#268](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/268) [#275](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/275)
- 添加示例说明如何调用 DataStream API 实现 sink 功能。[#253](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/253)
