# 从 AutoMQ Kafka 持续导入

[AutoMQ for Kafka](https://docs.automq.com/zh/docs/automq-s3kafka/YUzOwI7AgiNIgDk1GJAcu6Uanog)(简称 AutoMQ Kafka ) 是一款基于云重新设计的云原生 Kafka。
AutoMQ Kafka [内核开源](https://github.com/AutoMQ/automq-for-kafka)并且100% 兼容 Kafka 协议，可以充分兑现云的红利。
相比自建 Apache Kafka，AutoMQ Kafka 在其云原生架构基础上实现的自动弹性、流量自平衡、秒级分区移动等特性可以为用户带来更低的总体拥有成本（TCO）。
本文将介绍如何通过 StarRocks Routine Load 将数据导入 AutoMQ Kafka。关于Routine Load的基本原理可以参考 [Routine Load 基本原理](https://docs.starrocks.io/zh/docs/loading/load_concept/strict_mode/#routine-load)。

## 环境准备

### 准备 StarRocks 以及测试数据

请确保自己已经准备好了可用的 StarRocks 集群。本文为了方便演示过程，参考 [使用 Docker 部署 StarRocks](../quick_start/deploy_with_docker.md) 在一台 Linux 机器上安装了作为 Demo 的 StarRocks 集群。
创建库和主键表的测试表:

```sql
create database automq_db;
create table users (
  id bigint NOT NULL,
  name string NOT NULL,
  timestamp string NULL,
  status string NULL
) PRIMARY KEY (id)
DISTRIBUTED BY HASH(id)
PROPERTIES (
  "replication_num" = "1",
  "enable_persistent_index" = "true"
);
```

### 准备 AutoMQ Kafka 环境和测试数据

参考 AutoMQ [快速入门](https://docs.automq.com/zh/docs/automq-s3kafka/VKpxwOPvciZmjGkHk5hcTz43nde)部署好 AutoMQ Kafka 集群，确保 AutoMQ Kafka 与 StarRocks 之间保持网络连通性。
在AutoMQ Kafka中快速创建一个名为 example_topic 的主题并向其中写入一条测试JSON数据，可以通过以下步骤实现：

#### **创建Topic**

使用Kafka的命令行工具来创建主题。你需要有Kafka环境的访问权限，并且确保Kafka服务正在运行。以下是创建主题的命令：

```bash
./kafka-topics.sh --create --topic exampleto_topic --bootstrap-server 10.0.96.4:9092  --partitions 1 --replication-factor 1
```

> 注意：将 topic 和 bootstarp-server 替换为你的Kafka服务器地址。

创建完topic可以用以下命令检查topic创建的结果

```bash
./kafka-topics.sh --describe example_topic --bootstrap-server 10.0.96.4:9092
```

#### **生成测试数据**

我将为你生成一条简单的JSON格式的测试数据，和前文的表需要对应：

```json
{
  "id": 1,
  "name": "testuser",
  "timestamp": "2023-11-10T12:00:00",
  "status": "active"
}
```

#### **写入测试数据**

使用Kafka的命令行工具或者编程方式将测试数据写入到example_topic。以下是使用命令行工具的一个示例：

```bash
echo '{"id": 1, "name": "testuser", "timestamp": "2023-11-10T12:00:00", "status": "active"}' | sh kafka-console-producer.sh --broker-list 10.0.96.4:9092 --topic example_topic
```

> 注意：将 topic 和 bootstarp-server 替换为你的Kafka服务器地址。

使用如下命令可以查看刚写入的topic数据：

```bash
sh kafka-console-consumer.sh --bootstrap-server 10.0.96.4:9092 --topic example_topic --from-beginning
```

## 创建 Routine Load 导入作业

在 StarRocks 命令行创建一个 Routine Load导入作业，可以对 AutoMQ Kafka topic内的数据进行持续导入：

```sql
CREATE ROUTINE LOAD automq_example_load ON users
COLUMNS(id, name, timestamp, status)
PROPERTIES
(
  "desired_concurrent_number" = "5",
  "format" = "json",
  "jsonpaths" = "[\"$.id\",\"$.name\",\"$.timestamp\",\"$.status\"]"
)
FROM KAFKA
(
  "kafka_broker_list" = "10.0.96.4:9092",
  "kafka_topic" = "example_topic",
  "kafka_partitions" = "0",
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
```

> 注意：将 kafka_broker_list 替换为你的Kafka服务器地址。

### 参数说明

#### **数据格式**

需要PROPERTIES子句的"format" = "json"中指定数据格式为 JSON。

#### **数据提取和转换**

如果需要指定源数据和目标表之间列的映射和转换关系，则可以配置 COLUMNS 和 jsonpaths 参数。
COLUMNS 中的列名对应**目标表**的列名，列的顺序对应**源数据**中的列顺序。jsonpaths 参数用于提取 JSON 数据中需要的字段数据，就像新生成的 CSV 数据一样。
然后 COLUMNS 参数对 jsonpaths 中的字段**按顺序**进行临时命名。
更多数据转换的说明，请参见[导入时实现数据转换](./Etl_in_loading.md)。
> 注意：如果每行一个 JSON 对象中 key 的名称和数量（顺序不需要对应）都能对应目标表中列，则无需配置 COLUMNS 。

## 验证数据导入

首先我们查看 Routine Load 导入作业的情况，确认 Routine Load 导入任务状态为 RUNNING：

```sql
show routine load\G;
```

然后查询 StarRocks 数据库中对应的表，我们可以看到数据已经被成功导入：

```sql
StarRocks > select * from users;
+------+--------------+---------------------+--------+
| id   | name         | timestamp           | status |
+------+--------------+---------------------+--------+
|    1 | testuser     | 2023-11-10T12:00:00 | active |
|    2 | testuser     | 2023-11-10T12:00:00 | active |
+------+--------------+---------------------+--------+
2 rows in set (0.01 sec)
```
