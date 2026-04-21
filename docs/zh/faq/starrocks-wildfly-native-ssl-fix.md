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

根因是 Wildfly JNI 调用 `Java_org_wildfly_openssl_SSLImpl_makeSSLContext0` 时加载了按 OpenSSL 3.x ABI 编译的 `libwfssl.so`。但在 `starrocks_be` 进程内，`libwfssl.so` 对 OpenSSL 符号的查找会**先命中 `starrocks_be` 二进制中静态链接进来的 OpenSSL 1.x 符号**（来自 StarRocks thirdparty），根本不会走到系统磁盘上的 OpenSSL 3.x 共享库。3.x 的调用点（例如 `SSL_CTX_new_ex`）被派发到 1.x 的实现，就会在 SSL 上下文初始化时使 JVM 崩溃。因此，**仅在宿主机上安装 OpenSSL 3.x 或其开发头文件无法修复该崩溃**——BE 进程内置的 OpenSSL 1.x 永远会先命中符号解析。

---

## Q1：什么是 Wildfly OpenSSL 本地库？

Hadoop 使用 [Wildfly OpenSSL](https://github.com/wildfly-security/wildfly-openssl) 将本地 OpenSSL 绑定到 Java 的 JSSE（Java Secure Socket Extension）API。

这使得 Hadoop 的 S3A、Azure Blob（ABFS）和 Azure Data Lake（ADL）连接器可以使用原生 OpenSSL 来处理 TLS/SSL 连接，而不是依赖 JVM 内置的 SSL 实现。在正常情况下，这可以提升高吞吐加密 I/O 场景的性能。

---

## Q2：Hadoop 3.4.3 出了什么问题？

Hadoop 3.4.3 为支持 OpenSSL 3.0，将 Wildfly OpenSSL 升级到 `2.2.5.Final`（参见 [HADOOP-19719](https://issues.apache.org/jira/browse/HADOOP-19719)）。新版本的本地库（`libwfssl.so`）**基于 GLIBC 2.34+ 编译，且按 OpenSSL 3.x ABI 调用**。而 `starrocks_be` 二进制当前仍从 thirdparty 静态链接 OpenSSL 1.x，二者结合导致以下两类不同的故障：

| 平台                          | GLIBC 版本 | 系统 OpenSSL 版本 | 故障表现                                                                                  |
| --------------------------- | -------- | ------------- | ------------------------------------------------------------------------------------- |
| **RHEL 8 / CentOS 8**       | 2.28     | 1.1.1         | **加载阶段失败**：`UnsatisfiedLinkError: GLIBC_2.34 not found`，`libwfssl.so` 根本无法加载。         |
| **Ubuntu 22.04**            | 2.35     | 3.0.2         | **运行期 ABI 冲突**：`libwfssl.so` 可以加载，但其 OpenSSL 符号查找会命中 `starrocks_be` 内部静态链接的 OpenSSL 1.x，而不是系统的 OpenSSL 3.x。最终在 `SSL_CTX_new_ex` 中崩溃。**安装 `libssl-dev` 无法修复该问题**。 |
| **RHEL 9 / Ubuntu 24.04**   | 2.34+    | 3.0+          | 与 Ubuntu 22.04 相同的运行期 ABI 冲突：BE 进程内静态 OpenSSL 1.x 会遮蔽系统的 OpenSSL 3.x。**安装 `openssl-devel` 同样无效**。 |

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

### 方案二：从 BE 包中删除 `wildfly-openssl` JAR

问题的根因是：`wildfly-openssl-*.Final.jar` 中附带的 `libwfssl.so` 按 **OpenSSL 3.x ABI** 编译，而 `starrocks_be` 仍从 StarRocks thirdparty **静态链接 OpenSSL 1.x**。在 BE 进程内，`libwfssl.so` 对 OpenSSL 符号的查找会先命中 `starrocks_be` 内置的 1.x 符号，根本走不到系统的 OpenSSL 3.x 共享库，3.x 调用派发到 1.x 实现就会在 `SSL_CTX_new_ex` 中崩溃。**在 Ubuntu 上执行 `apt install libssl-dev`、或在 RHEL 上执行 `yum install openssl-devel` 都无法改变这一点**：即便这些包把宿主机上的 OpenSSL 升级到 3.x，BE 进程内的静态 OpenSSL 1.x 仍然会在符号解析中胜出。早期文档中若出现安装 `libssl-dev` / `openssl-devel` 的建议，请视为对本崩溃无效。

可靠的规避方式是直接删除该 JAR，使 Hadoop 根本无法加载该本地库，自动回退到 JSSE：

```bash
rm -f $STARROCKS_HOME/lib/hadoop/common/wildfly-openssl-2.2.5.Final.jar
```

请在每个 CN/BE 节点上执行（若部署了 Broker，也请在每个 Broker 节点上执行），然后重启对应服务。

从包含 [issue #71898](https://github.com/StarRocks/starrocks/issues/71898) 修复的 StarRocks 构建开始，该依赖已在 `java-extensions/pom.xml` 中针对 `hadoop-common`、`hadoop-aws`、`hadoop-azure`、`hadoop-azure-datalake` 四个依赖显式声明 `<exclusion>`，因此 `java-extensions/hadoop-lib` 不会再把该 JAR 打包给 BE。作为兜底保障，`build.sh` 也会 `rm -rf ${STARROCKS_OUTPUT}/be/lib/hadoop/common/wildfly-openssl-2.2.5.Final.jar` 以移除残留文件。升级后无需手动清理。

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

1. **在 Java 扩展层排除 `wildfly-openssl`**（[issue #71898](https://github.com/StarRocks/starrocks/issues/71898)）：
   最新 StarRocks 构建在 `java-extensions/pom.xml` 中针对 `hadoop-common`、`hadoop-aws`、`hadoop-azure`、`hadoop-azure-datalake` 均添加了 `<exclusion>org.wildfly.openssl:wildfly-openssl</exclusion>`，因此 `java-extensions/hadoop-lib` 不会再把该 JAR 打包给 BE。作为兜底，`build.sh` 仍会从 `${STARROCKS_OUTPUT}/be/lib/hadoop/common/` 中 `rm -rf` 残留的 `wildfly-openssl-2.2.5.Final.jar`。该排除是**临时性**的：待 StarRocks thirdparty 的 OpenSSL 升级到 3.x 后，BE 进程内部的 OpenSSL ABI 将与 `libwfssl.so` 的预期一致，届时会恢复该依赖。
2. **文档完善**：提供本 FAQ 及 `core-site.xml` 配置指导

> 说明：此前曾尝试在 Ubuntu 运行时 Docker 镜像中安装 `libssl-dev`（[PR #70688](https://github.com/StarRocks/starrocks/pull/70688)），但崩溃的真正原因是 `libwfssl.so` 的 OpenSSL 3.x 调用派发到了 **`starrocks_be` 中静态链接的 OpenSSL 1.x**，而不是宿主机缺少 OpenSSL 包，因此无论安装 `libssl-dev` 还是 `openssl-devel` 都无法改变结果。该方案已被上面的依赖排除方案替代。

---

## 快速修复（推荐）

在每个 CN/BE 节点（以及每个 Broker 节点，如果部署了 Broker）上，**任选以下其中一个**方案，然后重启服务。**请勿依赖 `apt install libssl-dev` 或 `yum install openssl-devel`——这些命令对本崩溃无效。**

**方案 A：删除 `wildfly-openssl` JAR**

```bash
rm -f $STARROCKS_HOME/lib/hadoop/common/wildfly-openssl-2.2.5.Final.jar
```

**方案 B：通过 `$STARROCKS_HOME/conf/core-site.xml` 强制使用 JSSE**

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
- [StarRocks Issue #71898: 从 BE 包中移除 `wildfly-openssl`](https://github.com/StarRocks/starrocks/issues/71898)
- [StarRocks PR #70688: Install libssl-dev for Ubuntu runtime（已被替代，无法修复该崩溃）](https://github.com/StarRocks/starrocks/pull/70688)
