---
displayed_sidebar: docs
sidebar_label: "Hadoop Wildfly Native SSL 库问题"
sidebar_position: 99
---
# FAQ：StarRocks 中 Hadoop 3.4.3 Wildfly Native SSL 库问题

## 背景

StarRocks 已将内置的 Hadoop 依赖从 3.4.2 升级到 3.4.3（[PR #69503](https://github.com/StarRocks/starrocks/pull/69503)）。Hadoop 3.4.3 引入了 [HADOOP-19719](https://issues.apache.org/jira/browse/HADOOP-19719)，将 Wildfly OpenSSL 绑定从 `2.1.4.Final` 升级到 `2.2.5.Final`，以支持 OpenSSL 3.0。然而，新版本的本地库（`libwfssl.so`）是基于 **GLIBC 2.34+** 构建的，这会在较旧的 Linux 发行版上引发兼容性问题。

**受影响的 StarRocks 版本：** 4.1.0+、4.0.7+、3.5.14+

### 典型现象

问题发生时，CN/BE 节点在初始化 SSL 上下文时可能因 **SIGSEGV（段错误）** 崩溃。FE 通常会报如下错误：

```
SQL Error [1064] [42000]: Access storage error. Error message: failed to get file schema:
A error occurred: errorCode=2001 errorMessage:Channel inactive error!
```

在 [GitHub Issue #70478](https://github.com/StarRocks/starrocks/issues/70478) 中，当通过 `FILES()` 函数查询 Azure Data Lake 上的 Parquet 文件时也会触发该问题。

```
*** SIGSEGV (@0x0) received by PID (TID 0x...) ***
    @     0x7b9b67093453 SSL_CTX_new_ex
    @     0x7b9b6a09d95a Java_org_wildfly_openssl_SSLImpl_makeSSLContext0
    @     0x7b9a68544be1 (unknown)
```

根因是 Wildfly JNI 调用 `Java_org_wildfly_openssl_SSLImpl_makeSSLContext0` 时进入 OpenSSL 本地代码，由于打包的 `libwfssl.so` 与系统 OpenSSL 在 ABI/运行时不兼容，从而导致崩溃。

---

## Q1：什么是 Wildfly OpenSSL 本地库？

Hadoop 使用 [Wildfly OpenSSL](https://github.com/wildfly-security/wildfly-openssl) 将本地 OpenSSL 绑定到 Java 的 JSSE（Java Secure Socket Extension）API。

这使得 Hadoop 的 S3A、Azure Blob（ABFS）和 Azure Data Lake（ADL）连接器可以使用原生 OpenSSL 来处理 TLS/SSL 连接，而不是依赖 JVM 内置的 SSL 实现。在正常情况下，这可以提升高吞吐加密 I/O 场景的性能。

---

## Q2：Hadoop 3.4.3 出了什么问题？

Hadoop 3.4.3 为支持 OpenSSL 3.0，将 Wildfly OpenSSL 升级到 `2.2.5.Final`（参见 [HADOOP-19719](https://issues.apache.org/jira/browse/HADOOP-19719)）。但新版本的本地库（`libwfssl.so`）是基于 **GLIBC 2.34+** 编译的，导致如下两类问题：

| 平台                                 | GLIBC 版本 | OpenSSL 版本 | 故障表现                                                   |
| ---------------------------------- | -------- | ---------- | ------------------------------------------------------ |
| **RHEL 8 / CentOS 8**              | 2.28     | 1.1.1      | `UnsatisfiedLinkError: GLIBC_2.34 not found`，本地库无法加载   |
| **Ubuntu 22.04**（未安装 `libssl-dev`） | 2.35     | 3.0.2      | 本地库可加载，但报 “file not found” 或 “interface not supported” |
| **RHEL 9 / Ubuntu 24.04**          | 2.34+    | 3.0+       | 通常可用，但缺少 OpenSSL 开发头文件时仍可能异常                           |

在受影响系统上，Wildfly 本地库问题可能导致：

- JVM 不稳定（CN/BE 产生 `hs_err` 崩溃日志）
- 无法建立到云存储（AWS S3、Azure Blob、Azure Data Lake）的 SSL/TLS 连接
- 静默回退到未加密连接或异常连接

---

## Q3：如何修复？

根据环境不同，可以选择以下解决方案：

### 方案一：禁用 Wildfly Native SSL（推荐）

强制 Hadoop 使用 JVM 内置的 SSL 实现（JSSE），而不是 Wildfly/OpenSSL。本方案**最安全、兼容性最好**。

在所有 CN/BE 节点的 `core-site.xml` 中添加配置：

```xml
<configuration>
    <!-- Disable Wildfly native SSL for AWS S3 (S3A connector) -->
    <property>
        <name>fs.s3a.ssl.channel.mode</name>
        <value>default_jsse</value>
    </property>

    <!-- Disable Wildfly native SSL for Azure Data Lake (ADL connector) -->
    <property>
        <name>adl.ssl.channel.mode</name>
        <value>Default_JSSE</value>
    </property>

    <!-- Disable Wildfly native SSL for Azure Blob Storage (ABFS connector) -->
    <property>
        <name>fs.azure.ssl.channel.mode</name>
        <value>Default_JSSE</value>
    </property>
</configuration>
```

**配置文件路径：**
- StarRocks BE/CN：`$STARROCKS_HOME/conf/core-site.xml`
- Broker 进程：`$BROKER_HOME/conf/core-site.xml`

修改后需**重启所有相关服务**。

---

### 方案二：安装 OpenSSL 开发头文件

在 Ubuntu 22.04+ 上，可以通过安装 OpenSSL 开发包解决部分问题：

```bash
sudo apt install libssl-dev
```

---

## Q4：哪些云存储连接器会受影响？

| 连接器                      | 配置项                         | 推荐值            |
| ------------------------ | --------------------------- | -------------- |
| **AWS S3（S3A）**          | `fs.s3a.ssl.channel.mode`   | `default_jsse` |
| **Azure Data Lake（ADL）** | `adl.ssl.channel.mode`      | `Default_JSSE` |
| **Azure Blob（ABFS）**     | `fs.azure.ssl.channel.mode` | `Default_JSSE` |

:::note
Azure Data Lake SDK 也支持通过 `AdlStoreOptions.setSSLChannelMode()` 进行编程配置，但在 StarRocks 中推荐使用 `core-site.xml` 方式。
:::

---

## Q5：禁用 Native SSL 是否影响性能？

会有一定影响，但对大多数场景影响较小。

- 使用 Wildfly + OpenSSL 的原生路径，在大规模加密 I/O 场景下性能更优
- JVM 内置的 JSSE 在现代 JDK（Java 11+）中已高度优化，能够满足大多数 StarRocks 数据湖查询需求

如果运行环境满足条件（如 RHEL 9 / Ubuntu 24.04，且具备 GLIBC 2.34+ 和 OpenSSL 3.0），可以继续使用 `default` 或 `openssl` 模式以获得最佳性能。

---

## Q6：如何验证修复是否生效？

完成配置并重启服务后：

1. **重新执行失败的任务**（例如查询外部 S3/Azure 表）
2. **检查 BE/CN 日志**，确认不存在以下内容：

   - `hs_err_pid*.log`（JVM 崩溃日志）
   - 包含 `GLIBC_2.34` 或 `libwfssl.so` 的 `UnsatisfiedLinkError`
   - `org.wildfly.openssl` 相关异常栈
3. **确认 SSL 连接正常建立**：

   - 使用 `default_jsse`：日志中应为标准 JSSE 握手信息（无 Wildfly 相关日志）
   - 使用 `default`：可能出现 `Failed to load OpenSSL. Falling back to the JSSE`（属正常现象）

---

## Q7：是否特别影响 RHEL 8 / CentOS 8？

是的。

RHEL 8 默认使用 **GLIBC 2.28**，明显低于 Wildfly 本地库要求的 2.34，因此：

- 本地库**无法加载**
- Hadoop 3.4.3 官方说明中明确指出：
  *由于本地库基于 GLIBC 2.34+ 构建，在 RHEL8 上不可用，应使用 JVM SSL 实现*

**建议：**

- 在 RHEL 8 上始终使用 `default_jsse`
- 长期建议升级到 RHEL 9

---

## Q8：FIPS 合规环境是否受影响？

可能受影响。

使用 FIPS 合规 SSL 库的 Linux 发行版，可能与 Wildfly OpenSSL 本地绑定不兼容。在此类环境中：

- **建议始终使用 `default_jsse`（JVM SSL）**
- 除非已验证本地库与 FIPS OpenSSL 完全兼容

---

## Q9：StarRocks 已采取哪些缓解措施？

1. **Docker 镜像修复**（[PR #70688](https://github.com/StarRocks/starrocks/pull/70688)）：
   在 Ubuntu 运行时镜像中安装 `libssl-dev`
2. **文档完善**：提供本 FAQ 及 `core-site.xml` 配置指导

---

## 快速修复（推荐）

在所有 CN/BE 节点的 `$STARROCKS_HOME/conf/core-site.xml` 中添加配置，并重启服务。

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.s3a.ssl.channel.mode</name>
        <value>default_jsse</value>
    </property>
    <property>
        <name>adl.ssl.channel.mode</name>
        <value>Default_JSSE</value>
    </property>
    <property>
        <name>fs.azure.ssl.channel.mode</name>
        <value>Default_JSSE</value>
    </property>
</configuration>
```

---

## 参考资料

- [HADOOP-19719: Upgrade to wildfly version with support for openssl 3](https://issues.apache.org/jira/browse/HADOOP-19719)
- [HADOOP-19262: Upgrade wildfly-openssl for JDK 17+](https://issues.apache.org/jira/browse/HADOOP-19262)
- [FLINK-38284: Flink downstream tracking issue](https://issues.apache.org/jira/browse/FLINK-38284)
- [Hadoop S3A Troubleshooting Guide](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/troubleshooting_s3a.html)
- [Hadoop S3A Performance Guide (SSL Channel Mode)](https://apache.github.io/hadoop/hadoop-aws/tools/hadoop-aws/performance.html)
- [Wildfly OpenSSL GitHub Repository](https://github.com/wildfly-security/wildfly-openssl)
- [StarRocks Issue #70478: SIGSEGV on Azure ADLS2 query (4.0.7)](https://github.com/StarRocks/starrocks/issues/70478)
- [StarRocks PR #69503: Upgrade Hadoop 3.4.2 to 3.4.3](https://github.com/StarRocks/starrocks/pull/69503)
- [StarRocks PR #70688: Install libssl-dev for Ubuntu runtime](https://github.com/StarRocks/starrocks/pull/70688)
