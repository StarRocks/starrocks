---
displayed_sidebar: docs
description: "使用 Arrow Flight SQL 从 StarRocks 获取最快大结果集读取的最佳实践与客户端代码模式。"
keywords: ['arrow flight sql', 'performance', 'best practices', 'optimization', 'parquet', 'jdbc']
---

# Arrow Flight SQL 最佳实践

Arrow Flight SQL 是从 StarRocks 中提取大型结果集的最快方式。在相同硬件和相同集群上，与 MySQL 协议相比，Arrow Flight 始终更快：**3×–9×**原始协议获取速度快**19×–97×**端到端到 pandas DataFrame 的速度更快。确切的倍数取决于行数、列结构以及所比较的 MySQL 客户端。但加速并非自动实现：客户端代码读取结果的方式对端到端时间有很大影响，一些简单的错误可能会抵消大部分性能提升。

本页展示了您可以预期的整体数据，总结了影响这些数据的各个方面，并结合代码变更和实测影响对每个方面进行了详细说明。

## 整体性能

以下进行两项比较。第一项仅衡量**协议获取**——字节到达并完成解析所需的时间，不涉及任何语言层面的对象转换。第二项衡量真实 Python 应用场景，其中数据被读入 `pandas` DataFrame。硬件信息请参见[测试环境](#test-environment)。

### 协议层获取（Arrow Flight ADBC 与 `mysql --quick` 对比）

`fetch_arrow_table()` 将网络数据直接写入 Arrow 缓冲区，无需将单元格转换为 Python 对象。`mysql --quick` 使用流式 C 客户端解析行，直接消费 MySQL 线协议。两者均为纯协议层操作——均不涉及语言原生对象的实例化开销。

| 工作负载 | 行数 | MySQL 协议<br />(`mysql --quick`) | Arrow Flight<br />(`fetch_arrow_table`) | 加速比 |
| --- | --- | --- | --- | --- |
| 单数值列 (`SELECT id`) | 1 M | 831 ms | 215 ms | **3.9×** |
| 单数值列 (`SELECT id`) | 5 M | 2,216 ms | 456 ms | **4.9×** |
| 单数值列 (`SELECT id`) | 10 M | 4,166 ms | 1,163 ms | **3.6×** |
| 单数值列 (`SELECT id`) | 100 M | 35,629 ms | 6,737 ms | **5.3×** |
| 20 个数值列 (`SELECT *`) | 1 M | 1,994 ms | 370 ms | **5.4×** |
| 20 个数值列 (`SELECT *`) | 5 M | 9,665 ms | 1,251 ms | **7.7×** |
| 20 个数值列 (`SELECT *`) | 10 M | 18,461 ms | 2,577 ms | **7.2×** |
| 20 个数值列 (`SELECT *`) | 100 M | 178,416 ms | 19,047 ms | **9.4×** |
| 20 个 VARCHAR 列 (`SELECT *`) | 1 M | 4,549 ms | 1,294 ms | **3.5×** |
| 20 个 VARCHAR 列 (`SELECT *`) | 5 M | 19,077 ms | 5,959 ms | **3.2×** |
| 20 个 VARCHAR 列 (`SELECT *`) | 10 M | 36,079 ms | 11,499 ms | **3.1×** |
| 20 个 VARCHAR 列 (`SELECT *`) | 100 M | 370,858 ms | 164,508 ms（分块） | **2.3×** |

### 真实 Python 应用场景 — `pd.read_sql` 对比 ADBC 与 PyMySQL

Python 的标准管道是 `pd.read_sql(sql, conn) → pandas.DataFrame`。传入的连接对象决定了整个迁移方式：传入 PyMySQL 的 `Connection`，pandas 会调用 `cursor.fetchall()` + `pd.DataFrame(rows)`，逐行遍历以构建 DataFrame。传入 ADBC Flight SQL 连接，pandas 则使用 ADBC 原生的 Arrow 获取方式，以及近零拷贝的 DataFrame 转换。

| 工作负载 | 行数 | `pd.read_sql(sql,`<br />`adbc_conn)` | `pd.read_sql(sql,`<br />`pymysql_conn)` | 加速比 |
| --- | --- | --- | --- | --- |
| 单数值列（`SELECT id`）| 1 M | 320 ms | 6,185 ms | **19.3×** |
| 单数值列（`SELECT id`）| 5 M | 421 ms | 30,751 ms | **73.0×** |
| 单数值列（`SELECT id`）| 10 M | 970 ms | 61,524 ms | **63.4×** |
| 单数值列（`SELECT id`）| 100 M | 6,024 ms | 585,556 ms | **97.2×** |
| 20 个数值列（`SELECT *`）| 1 M | 522 ms | 27,521 ms | **52.7×** |
| 20 个数值列（`SELECT *`）| 5 M | 1,530 ms | 141,500 ms | **92.5×** |
| 20 个数值列（`SELECT *`）| 10 M | 2,689 ms | 255,408 ms | **95.0×** |
| 20 个数值列（`SELECT *`）| 100 M | 24,568 ms | OOM | — |
| 20 个 VARCHAR 列（`SELECT *`）| 1 M | 1,560 ms | 31,407 ms | **20.1×** |
| 20 个 VARCHAR 列（`SELECT *`）| 5 M | 6,937 ms | 154,560 ms | **22.3×** |
| 20 个 VARCHAR 列（`SELECT *`）| 10 M | 13,260 ms | 304,647 ms | **23.0×** |

每个单元格为*获取* + *转换* = *总计*；加速比为总时间之比。窄数值查询的加速比最大，原因在于 PyMySQL 在获取数据时会为每个单元格分配一个 Python `int` 对象，pandas 在转换时还需遍历元组列表——而 ADBC 两项开销均可跳过。Arrow 的列式内存格式双重获益：在获取阶段跳过了逐单元格的 Python 对象分配，并使后续的 DataFrame 转换几乎零开销。

如果您现有代码已使用 `pd.read_sql`，迁移只需一行：

```python
import adbc_driver_manager
import adbc_driver_flightsql.dbapi as fl
import pandas as pd

with fl.connect(
        uri="grpcs://host:443",
        db_kwargs={
            adbc_driver_manager.DatabaseOptions.USERNAME.value: "admin",
            adbc_driver_manager.DatabaseOptions.PASSWORD.value: "...",
        }) as conn:
    df = pd.read_sql("SELECT * FROM my_table LIMIT 5000000", conn)
```

## 测试环境

| 组件 | 详情                                                                                                                                      |
| --- |----------------------------------------------------------------------------------------------------------------------------------------------|
| 客户端主机 | AWS EC2 `t3.2xlarge`，与集群位于同一 VPC 子网                                                                                         |
| 集群 | 3 FE + 2 BE，运行于 `m6g.xlarge`；Arrow Flight 端口 `grpcs://…:443`，MySQL 端口 `:9030`                                                               |
| Java 栈 | OpenJDK 17，`jdbc:arrow-flight-sql`，`arrow-jdbc`，`parquet-hadoop`                                                                          |
| Python 栈 | `python` 3.12，`pyarrow` 24.0，`adbc-driver-flightsql` 1.11，`PyMySQL` 1.2                                                                   |
| 工作负载 | 两张 20 列的表——一张以 VARCHAR 为主，一张全为整数——加上单列投影；行数分别为 1 M、5 M、10 M 和 100 M，通过 `SELECT … LIMIT N` 生成 |
| MySQL 排空模式 | 所有测量均使用 `cursor.fetchall()` 缓冲模式                                                                                            |

## 选择客户端

在进行任何代码级调优之前，最重要的单一决策是选择通过哪种客户端 API 读取结果。[通过 Arrow Flight SQL 与 StarRocks 交互](./arrow_flight.md) 涵盖了 Python ADBC、Arrow Flight JDBC 驱动、Java ADBC 驱动以及原生 `FlightClient` 的完整配置。从性能角度来看，这些方案归结为两条路径：

- **通过 `FlightSqlClient` 或ADBC（推荐）获取原始Arrow批次。** 这是 Flight SQL 协议专为其设计的列式端到端路径：您的代码接收 `VectorSchemaRoot` 批次，并使用返回原始值的向量访问器读取它们，无需按行分配对象。端到端（排空 + 类型转换），此路径大约为 **10×** 在 1000 万行数字数据上比 Java MySQL JDBC 更快，并且 **高达 97×**在将1亿条窄数字查询以pandas DataFrame形式返回时，速度比PyMySQL更快。只要下游代码能够消费列式数据（Pandas、Arrow、ML流水线、Parquet写入器、自定义分析），就应优先使用它。
- **Arrow Flight JDBC 驱动程序（`jdbc:arrow-flight-sql`）。**当您需要为现有 JDBC 代码路径提供即插即用的 `ResultSet`，或用于 Tableau、Power BI、DBeaver 等需要 JDBC 接口的 BI 工具时，请使用此选项。JDBC 的 API 强制驱动程序为每个单元格返回装箱的 `Object`，因此此路径无法达到原始 Arrow 批次的性能。JDBC 驱动程序仍然比 MySQL JDBC 快得多；当 JDBC 兼容性是需求时，它是正确的工具。

下方各方面的表格切换了基准：它们将 Java Arrow Flight JDBC 驱动程序与 Java MySQL JDBC 进行比较，而非与 PyMySQL 比较。Java MySQL JDBC 连接器在行实例化方面比 PyMySQL 快得多——例如，同样 500 万行 VARCHAR `SELECT *` 通过 Java MySQL JDBC 需要约 22 秒，而通过 PyMySQL 则需要约 105 秒——因此您将看到的 Java 比率小于整体性能中的 Python 数字。当您在 Java 驱动程序之间进行选择时，Java MySQL JDBC 是正确的基准。

以下四个方面适用于您选择的任何客户端：方面1适用于JDBC消费者，方面2–3适用于原始批量消费者，方面4涵盖来自任一方式的Parquet输出。

## 影响性能的因素

The speedups above assume the client code is written for Arrow. The following four aspects each move the needle by 2× or more on the right workload. Getting them right is the difference between the "tuned" column in the table above and a fetch that looks no faster than MySQL.

1. **JDBC 访问器方法。**对数值列使用带类型转换的 `rs.getObject(i)`。`rs.getString(i)` 会强制驱动程序将每个值格式化为字符串。
2. **向量解析范围。**在消费原始 `VectorSchemaRoot` 批次时，在行循环外每批次解析一次 `FieldVector`，而不是每行解析一次。
3. **为数字类型输入了 `.get(i)`。** 在数值向量上，类型化的 `.get(i)` 返回一个无需分配的原始值。通用访问器会对每个值进行装箱。
4. **Parquet 写入器选择。** PyArrow 直接从 Arrow 流写入 Parquet，无需逐行代码。Java 没有预置库来实现这一点——每条 Java 路径都需要在 `parquet-hadoop` 之上手写 `WriteSupport<VectorSchemaRoot>`。

### 方面 1 — JDBC：使用类型化列访问

使用 Arrow Flight JDBC 驱动程序时，请使用 `rs.getObject(i)` 并转换为预期的 Java 类型。这样驱动程序可以直接返回原生 Java 类型，无需额外的转换步骤，这对数值列尤为重要。

```java
while (rs.next()) {
    Integer id   = (Integer) rs.getObject(1);
    String  name = (String)  rs.getObject(2);
    Long    ts   = (Long)    rs.getObject(3);
}
```

### 基准测试：JDBC 访问器方法（包含网络）

| 工作负载 | MySQL JDBC | Arrow Flight JDBC，类型化 `getObject()` | 加速比 |
| --- | --- | --- | --- |
| VARCHAR — 5 M | 22,651 ms | 12,660 ms | **1.79×** |
| VARCHAR — 10 M | 49,216 ms | 27,646 ms | **1.78×** |
| 数字 — 5 M | 16,043 ms | 3,123 ms | **5.14×** |
| 数字 — 10 M | 38,134 ms | 9,123 ms | **4.18×** |

### 方面 2 — 原始 Arrow 批次：预解析向量并使用类型化访问

通过原生 `FlightSqlClient` 消费原始 Arrow 批次时（即迭代 `VectorSchemaRoot` 对象），请遵循以下两条规则。

**每批次解析一次向量，而非每行解析一次。** 在行循环之前调用 `root.getVector("column_name")`，这样查找成本每批次只需支付一次，而不是每行支付一次。

**对数值向量使用类型化的 .get(i)。** 这直接返回一个原始值——无堆分配，无 GC 压力。

```java
IntVector      idVec    = (IntVector)      root.getVector("id");
SmallIntVector yearVec  = (SmallIntVector) root.getVector("birth_year");
TinyIntVector  monthVec = (TinyIntVector)  root.getVector("birth_month");

for (int i = 0; i < rowCount; i++) {
    record.id         = idVec.get(i);     // int   — no allocation
    record.birthYear  = yearVec.get(i);   // short — no allocation
    record.birthMonth = monthVec.get(i);  // byte  — no allocation
}
```

### 基准测试：Arrow 批量转换成本（预取）

下方的 Arrow Flight 数字单独衡量了转换开销：批次先从集群排入内存，然后再计时。

| 工作负载 | MySQL JDBC | 类型化 `.get*()`，每批次解析一次向量 | 加速比 |
| --- | --- | --- | --- |
| VARCHAR — 5 M | 22,651 ms | 11,921 ms | **1.90×** |
| VARCHAR — 10 M | 49,216 ms | 24,686 ms | **1.99×** |
| Numeric — 5 M | 16,043 ms | 1,141 ms | **14.1×** |
| Numeric — 10 M | 38,134 ms | 2,092 ms | **18.2×** |

### 方面 3 — 将结果写入 Parquet

Apache Arrow 不包含适用于 `VectorSchemaRoot` 的预构建 Parquet 写入器。如果您的目标仅是将查询结果导出为 Parquet 文件，[INSERT INTO FILES](./unload_using_insert_into_files.md) 可让 StarRocks 在服务端写入文件，无需任何客户端转换代码。以下选项适用于需要通过 Arrow Flight 在客户端输出 Parquet 的场景。

### 选项 1：使用 PyArrow 的 Python（推荐）

PyArrow 无需自定义写入逻辑即可处理 Arrow → Parquet 的转换，并原生保留列类型（INT32、INT64、TIMESTAMP_MICROS 等）。

**从 Arrow Flight 逐批流式传输：**

```python
import pyarrow.flight as fl
import pyarrow.parquet as pq

client = fl.connect("grpc+tls://host:443")
info   = client.get_flight_info(
    fl.FlightDescriptor.for_command(b"SELECT ..."))

reader = client.do_get(info.endpoints[0].ticket)
with pq.ParquetWriter("output.parquet", reader.schema_arrow, compression="snappy") as writer:
    for batch in reader:
        writer.write_batch(batch)
```

**如果完整结果可放入内存：**

```python
table = reader.read_all()
pq.write_table(table, "output.parquet", compression="snappy")
```

**通过 ADBC（推荐的 Python Flight SQL 客户端）：**

```python
import adbc_driver_flightsql.dbapi as fl_sql
import pyarrow.parquet as pq

with fl_sql.connect("grpcs://host:443", db_kwargs={"username": "admin", "password": "..."}) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM my_table LIMIT 5000000")
        pq.write_table(cur.fetch_arrow_table(), "output.parquet", compression="snappy")
```

### 选项 2：Java WriteSupport

对于 Java，在 `org.apache.parquet:parquet-hadoop` 之上构建自定义 `WriteSupport<VectorSchemaRoot>`。每个作业构建一次 schema 和 writer，然后在 `WriteSupport.write()` 内使用类型化向量读取。

**构建 schema 和 writer 一次：**

```java
MessageType parquetSchema = new SchemaConverter().fromArrow(arrowSchema).getParquetSchema();
ParquetWriter<VectorSchemaRoot> writer = /* build once per job */;

// 每批次：
writer.write(batch);
```

**在 `WriteSupport.write()` 内使用类型化读取：**

```java
class ArrowWriteSupport extends WriteSupport<VectorSchemaRoot> {
    private RecordConsumer recordConsumer;

    @Override
    public void prepareForWrite(RecordConsumer consumer) {
        this.recordConsumer = consumer;
    }

    @Override
    public void write(VectorSchemaRoot root) {
        int rowCount = root.getRowCount();
        for (FieldVector vec : root.getFieldVectors()) {
            if (vec instanceof IntVector) {
                IntVector iv = (IntVector) vec;
                for (int row = 0; row < rowCount; row++) {
                    recordConsumer.addInteger(iv.get(row));
                }
            } // else if (vec instanceof SmallIntVector) ... BigIntVector ... VarCharVector ...
        }
    }
}
```

### Parquet 基准测试

数字包含 Parquet 编码和文件 I/O 开销（参见 [测试环境](#test-environment)）。VARCHAR 和数值表分别进行基准测试，因为它们对 Arrow 编码路径的不同部分施加压力：VARCHAR 列需要对可变长度数据进行偏移缓冲区运算，而数值列使用固定宽度的类型化向量，类型化访问带来的收益更大。

#### Java（5 M 和 10 M 行）

两行使用相同的 `parquet-hadoop` 写入路径（`MySqlParquetConverter` + `arrow-jdbc` 适配器，批次大小 65,536），因此唯一的变量是入站 JDBC 驱动程序。

| 方法 | 行数 | VARCHAR 无压缩 | 对比 MySQL | VARCHAR Snappy | 对比 MySQL | Numeric 无压缩 | 对比 MySQL | Numeric Snappy | 对比 MySQL |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MySQL JDBC → Parquet | 5 M | 55,477 ms | 1.0× | 54,888 ms | 1.0× | 24,006 ms | 1.0× | 25,289 ms | 1.0× |
| Arrow Flight JDBC → Parquet | 5 M | **46,341 毫秒** | **1.20×** | **47,881 毫秒** | **1.15×** | **13,978 毫秒** | **1.72×** | **14,297 毫秒** | **1.77×** |
| MySQL JDBC → Parquet | 10 M | 110,229 ms | 1.0× | 116,999 ms | 1.0× | 50,509 ms | 1.0× | 49,126 ms | 1.0× |
| Arrow Flight JDBC → Parquet | 10 M | **91,386 ms** | **1.21×** | **102,534 毫秒** | **1.14×** | **29,739 毫秒** | **1.70×** | **30,102 毫秒** | **1.63×** |

#### Python (PyArrow 24.0.0 / ADBC 1.11.0)

MySQL 基准与上表中 Java MySQL JDBC → Parquet 的数字相同；"MySQL → PyArrow" 并非真实路径，因为在 `arrow-jdbc` 之外不存在 MySQL → Arrow 适配器。Python 数字仅在 500 万条记录时收集。

| 方法 | VARCHAR 未压缩 | 对比 MySQL | VARCHAR Snappy | 对比 MySQL | 数值型 未压缩 | 对比 MySQL | 数值型 Snappy | 对比 MySQL |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MySQL JDBC → Parquet（Java 基准，500 万）| 55,477 ms | 1.0× | 54,888 ms | 1.0× | 24,006 ms | 1.0× | 25,289 ms | 1.0× |
| Arrow Flight + PyArrow（500 万）| **10,675 毫秒** | **5.20×** | **14,128 毫秒** | **3.89×** | **3,953 毫秒** | **6.07×** | **3,848 毫秒** | **6.57×** |

PyArrow 在原始网络获取的基础上几乎不增加额外开销，且所需代码远少于 Java 路径。除非 Java 是硬性要求，否则请使用 PyArrow。

### 总结

| 使用场景 | 建议 |
| --- | --- |
| Arrow Flight JDBC | 使用带类型转换的 `getObject()` |
| 原始 `VectorSchemaRoot` 批次 | 每批次解析一次向量；对数值列使用类型化的 `.get(i)` |
| Python 中 Arrow → Parquet | 通过 ADBC 使用 `pyarrow.parquet` — 单函数调用，无需自定义代码 |
| Java 中 Arrow → Parquet | 手写 `WriteSupport<VectorSchemaRoot>` 并使用类型化向量读取 |
