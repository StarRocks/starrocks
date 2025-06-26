---
displayed_sidebar: docs
---

# SSB 扁平表基准测试

星形模型基准测试（SSB）旨在测试 OLAP 数据库产品的基本性能指标。SSB 使用一种在学术界和工业界广泛应用的星形模型测试集。有关更多信息，请参见 [Star Schema Benchmark](https://www.cs.umb.edu/~poneil/StarSchemaB.PDF)。

ClickHouse 将星形模型展平为一个宽扁平表，并将 SSB 重写为单表基准测试。有关更多信息，请参见 [Star schema benchmark of ClickHouse](https://clickhouse.com/docs/en/getting-started/example-datasets/star-schema)

此测试比较了 StarRocks、Apache Druid 和 ClickHouse 在 SSB 单表数据集上的性能。

## 测试结论

测试在一个共享无状态的 StarRocks 集群上的 OLAP 表中进行，同时与 ClickHouse 和 Apache Druid 使用相同的数据集进行比较。

基于对 100 GB SSB-Flat 数据集进行的 13 个查询的结果，StarRocks 的整体查询性能是 **ClickHouse 的 1.87 倍和 Apache Druid 的 4.75 倍**。结果的单位是毫秒。

![SSB-SR](../_assets/benchmark/SSB-SR.png)

## 测试准备

### 硬件

StarRocks、Apache Druid 和 ClickHouse 部署在相同配置的主机上 - [AWS m7i.4xlarge](https://aws.amazon.com/ec2/instance-types/m7i/?nc1=h_ls)。

|                          | **规格**    |
| ------------------------ | ----------- |
| 实例数量                 | 5           |
| vCPU                     | 16          |
| 内存 (GiB)               | 64          |
| 网络带宽 (Gbps)          | 高达 12.5   |
| EBS 带宽 (Gbps)          | 高达 10     |

### 软件

|                   | **StarRocks**     | **ClickHouse** | **Apache Druid**                                             |
| ----------------- | ----------------- | -------------- | ------------------------------------------------------------ |
| **集群规模**      | 一个 FE，三个 BEs | 三个节点       | 一个主服务器，一个查询服务器和三个数据服务器               |
| **版本**          | 3.5.0             | 25.3.3.42      | 33.0.0                                                       |
| **发布日期**      | 2025.6.13         | 2025.4.22      | 2025.4.29                                                    |
| **配置**          | 默认              | 默认           | 默认                                                         |

## 测试结果

下表显示了 13 个查询的性能测试结果。查询延迟的单位是毫秒。所有查询预热 1 次，然后执行 3 次取平均值作为结果。表头中的 `ClickHouse vs StarRocks` 和 `Druid vs StarRocks` 表示使用 ClickHouse/Druid 的查询响应时间除以 StarRocks 的查询响应时间。值越大表示 StarRocks 的性能越好。

| 查询  | StarRocks | ClickHouse | Druid | ClickHouse vs StarRocks | Druid vs StarRocks |
| ----- | --------- | ---------- | ----- | ----------------------- | ------------------ |
| SUM   | 992       | 1858       | 4710  | 1.87                    | 4.75               |
| Q01   | 30        | 49         | 330   | 1.63                    | 11.00              |
| Q02   | 16        | 31         | 260   | 1.94                    | 16.25              |
| Q03   | 26        | 29         | 250   | 1.12                    | 9.62               |
| Q04   | 143       | 197        | 420   | 1.38                    | 2.94               |
| Q05   | 120       | 179        | 440   | 1.49                    | 3.67               |
| Q06   | 63        | 158        | 320   | 2.51                    | 5.08               |
| Q07   | 133       | 249        | 510   | 1.87                    | 3.83               |
| Q08   | 90        | 197        | 380   | 2.19                    | 4.22               |
| Q09   | 86        | 150        | 350   | 1.74                    | 4.07               |
| Q10   | 20        | 33         | 250   | 1.65                    | 12.50              |
| Q11   | 156       | 340        | 550   | 2.18                    | 3.53               |
| Q12   | 66        | 133        | 330   | 2.02                    | 5.00               |
| Q13   | 43        | 113        | 320   | 2.63                    | 7.44               |