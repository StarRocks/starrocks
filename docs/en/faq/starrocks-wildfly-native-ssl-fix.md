---
displayed_sidebar: docs
sidebar_label: "Hadoop Wildfly Native SSL Library Issue"
sidebar_position: 99
---
# FAQ: Hadoop 3.4.3 Wildfly Native SSL Library Issue in StarRocks

## Background

StarRocks upgraded its bundled Hadoop dependency from 3.4.2 to 3.4.3 ([PR #69503](https://github.com/StarRocks/starrocks/pull/69503)). Hadoop 3.4.3 includes [HADOOP-19719](https://issues.apache.org/jira/browse/HADOOP-19719), which upgrades the Wildfly OpenSSL bindings from `2.1.4.Final` to `2.2.5.Final` to add OpenSSL 3.0 compatibility. However, the new native libraries (`libwfssl.so`) were built against **GLIBC 2.34+**, which introduces compatibility issues on older Linux distributions.

**Affected StarRocks versions:** 4.1.0+, 4.0.7+, 3.5.14+

### Typical Symptoms

When this issue occurs, CN/BE nodes crash with a **SIGSEGV** (segmentation fault) during SSL context initialization. The FE reports errors like:

```
SQL Error [1064] [42000]: Access storage error. Error message: failed to get file schema:
A error occurred: errorCode=2001 errorMessage:Channel inactive error!
```

From [GitHub Issue #70478](https://github.com/StarRocks/starrocks/issues/70478) — querying Parquet files on Azure Data Lake via `FILES()` function:

```
*** SIGSEGV (@0x0) received by PID (TID 0x...) ***
    @     0x7b9b67093453 SSL_CTX_new_ex
    @     0x7b9b6a09d95a Java_org_wildfly_openssl_SSLImpl_makeSSLContext0
    @     0x7b9a68544be1 (unknown)
```

The Wildfly JNI call `Java_org_wildfly_openssl_SSLImpl_makeSSLContext0` loads `libwfssl.so`, which is compiled against the OpenSSL 3.x ABI. Inside the `starrocks_be` process, however, OpenSSL symbol lookups from `libwfssl.so` are satisfied by the **OpenSSL 1.x symbols that are statically linked into the `starrocks_be` binary** (from StarRocks' thirdparty) before they can ever reach the system's OpenSSL 3.x shared libraries on disk. A 3.x call site (`SSL_CTX_new_ex`) dispatching into a 1.x implementation is what actually crashes the JVM. This is why merely installing OpenSSL 3.x or its development headers on the host does NOT fix the crash -- the BE's in-process OpenSSL 1.x always wins symbol resolution.

---

## Q1: What is the Wildfly OpenSSL native library?

Hadoop uses the [Wildfly OpenSSL](https://github.com/wildfly-security/wildfly-openssl) library to bind native OpenSSL to the Java JSSE (Java Secure Socket Extension) APIs. This allows Hadoop's S3A, Azure Blob (ABFS), and Azure Data Lake (ADL) connectors to use native OpenSSL for TLS/SSL connections instead of the JVM's built-in SSL implementation. When it works, this provides a performance benefit for high-throughput encrypted I/O.

---

## Q2: What went wrong in Hadoop 3.4.3?

Hadoop 3.4.3 upgraded Wildfly OpenSSL to `2.2.5.Final` to support OpenSSL 3.0 ([HADOOP-19719](https://issues.apache.org/jira/browse/HADOOP-19719)). The new native library (`libwfssl.so`) was built against **GLIBC 2.34+** and targets the **OpenSSL 3.x ABI**. Because StarRocks' `starrocks_be` binary still statically links OpenSSL 1.x from its thirdparty, this produces two distinct failure modes:

| Platform | GLIBC Version | System OpenSSL | Failure Mode |
|----------|--------------|-----------------|--------------|
| **RHEL 8 / CentOS 8** | 2.28 | 1.1.1 | **Load-time failure**: `UnsatisfiedLinkError: GLIBC_2.34 not found` -- `libwfssl.so` cannot load at all. |
| **Ubuntu 22.04** | 2.35 | 3.0.2 | **Runtime ABI collision**: `libwfssl.so` loads, but its OpenSSL symbol lookups resolve to the static OpenSSL 1.x embedded in `starrocks_be`, not the system's OpenSSL 3.x. Crashes in `SSL_CTX_new_ex`. Installing `libssl-dev` does NOT fix this. |
| **RHEL 9 / Ubuntu 24.04** | 2.34+ | 3.0+ | Same runtime ABI collision as Ubuntu 22.04: the BE's in-process OpenSSL 1.x shadows the system OpenSSL 3.x. Installing `openssl-devel` does NOT fix this either. |

On affected systems, the Wildfly native library crash can cause JVM instability (including `hs_err` crash logs on CN/BE nodes), failed SSL/TLS connections to cloud storage (AWS S3, Azure Blob, Azure Data Lake), and silent fallback to unencrypted or broken connections.

---

## Q3: How do I fix this?

There are multiple solutions depending on your environment and requirements.

### Solution 1: Disable Wildfly Native SSL

Force Hadoop to use the JVM's built-in SSL implementation instead of the native Wildfly/OpenSSL path. This is the **safest and most portable fix**.

Add the following to your `core-site.xml` on **all CN/BE nodes**:

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

**Where to place `core-site.xml`:**
- For StarRocks BE/CN: `$STARROCKS_HOME/conf/core-site.xml`
- For Broker processes: `$BROKER_HOME/conf/core-site.xml`

**Restart all affected services** after making the change.

### Solution 2: Remove the `wildfly-openssl` JAR from the BE Package

The root cause is that `libwfssl.so` (shipped inside `wildfly-openssl-*.Final.jar`) is compiled for the OpenSSL 3.x ABI, while the `starrocks_be` binary still statically links OpenSSL 1.x from the StarRocks thirdparty. Inside the BE process, OpenSSL symbol lookups from `libwfssl.so` are resolved against the 1.x symbols baked into `starrocks_be` before they can reach any system OpenSSL 3.x shared library, and the 3.x-call-into-1.x-implementation dispatch crashes in `SSL_CTX_new_ex`. Installing `libssl-dev` on Ubuntu or `openssl-devel` on RHEL cannot change this: even if those packages upgrade the host's OpenSSL to 3.x, the BE's in-process OpenSSL 1.x still wins symbol resolution every time. Treat any older guidance that suggests `apt install libssl-dev` or `yum install openssl-devel` as ineffective for this crash.

A reliable workaround is to delete the offending JAR so Hadoop cannot load the native library at all and falls back to JSSE automatically:

```bash
rm -f $STARROCKS_HOME/lib/hadoop/common/wildfly-openssl-2.2.5.Final.jar
```

Run this on every CN/BE node (and on every Broker node if a Broker is deployed) and restart the service.

Starting from StarRocks builds that include the fix for [issue #71898](https://github.com/StarRocks/starrocks/issues/71898), the dependency is excluded at the Maven level in `java-extensions/pom.xml` (for `hadoop-common`, `hadoop-aws`, `hadoop-azure`, and `hadoop-azure-datalake`), so the JAR is never produced by the `java-extensions/hadoop-lib` assembly. `build.sh` additionally `rm -rf`s `${STARROCKS_OUTPUT}/be/lib/hadoop/common/wildfly-openssl-2.2.5.Final.jar` from the BE output as a belt-and-braces safety net. No manual cleanup is required after upgrading.

---

## Q4: Which cloud storage connectors are affected?

| Connector | Configuration Property | Recommended Value |
|-----------|----------------------|-------------------|
| **AWS S3** (S3A) | `fs.s3a.ssl.channel.mode` | `default_jsse` |
| **Azure Data Lake** (ADL) | `adl.ssl.channel.mode` | `Default_JSSE` |
| **Azure Blob Storage** (ABFS) | `fs.azure.ssl.channel.mode` | `Default_JSSE` |

:::note
The Azure Data Lake Store SDK also supports `AdlStoreOptions.setSSLChannelMode()` programmatically, but for StarRocks the `core-site.xml` approach is the standard method.
:::

---

## Q5: Is there a performance impact from disabling native SSL?

Yes, but it is minor for most workloads. The native OpenSSL path (via Wildfly) can provide better throughput for large-scale encrypted I/O operations. However, the JVM's built-in JSSE implementation is well-optimized in modern JDKs (Java 11+) and is sufficient for most StarRocks data lake query patterns.

If you are on a platform that supports the new Wildfly native library (RHEL 9 / Ubuntu 24.04 with GLIBC 2.34+ and OpenSSL 3.0), you can keep the `default` or `openssl` mode for maximum performance.

---

## Q6: How do I verify the fix is working?

After applying the configuration change and restarting services:

1. **Re-run the failing workload** (e.g., query against external S3/Azure tables).
2. **Check BE/CN logs** for the absence of:
   - `hs_err_pid*.log` files (JVM crash logs)
   - `UnsatisfiedLinkError` mentioning `GLIBC_2.34` or `libwfssl.so`
   - `org.wildfly.openssl` error stack traces
3. **Confirm successful SSL connections** in the log:
   - With `default_jsse`: You should see standard JSSE handshake logs (no Wildfly references).
   - With `default`: You may see `Failed to load OpenSSL. Falling back to the JSSE` (this is expected and safe).

---

## Q7: Does this affect RHEL 8 / CentOS 8 specifically?

Yes. RHEL 8 ships with **GLIBC 2.28**, which is significantly older than the 2.34 required by the new Wildfly native library. On RHEL 8:

- The native library **will not load at all** (`UnsatisfiedLinkError`).
- The Hadoop 3.4.3 release notes explicitly state: *"as the native libraries were built with GLIBC 2.34+, these do not work on RHEL8. For deployment on those systems, stick with the JVM ssl support."*

**Recommendation for RHEL 8:** Always use `default_jsse` mode. Plan migration to RHEL 9 for long-term support.

---

## Q8: What about FIPS-compliant environments?

Linux distributions with a FIPS-compliant SSL library may not be compatible with the Wildfly native OpenSSL bindings. If you are running in a FIPS-compliant environment, **always use `default_jsse`** (JVM SSL) unless you have verified that the native library is compatible with your specific FIPS OpenSSL implementation.

---

## Q9: What has StarRocks done to mitigate this?

1. **Exclude `wildfly-openssl` at the Java-extensions build layer** ([issue #71898](https://github.com/StarRocks/starrocks/issues/71898)): recent StarRocks builds add `<exclusion>org.wildfly.openssl:wildfly-openssl</exclusion>` to the `hadoop-common`, `hadoop-aws`, `hadoop-azure`, and `hadoop-azure-datalake` dependencies in `java-extensions/pom.xml`, so `java-extensions/hadoop-lib` no longer ships the JAR to the BE at all. For extra safety, `build.sh` also `rm -rf`s any straggling `wildfly-openssl-2.2.5.Final.jar` from `${STARROCKS_OUTPUT}/be/lib/hadoop/common/`. The exclusion is intentionally temporary: it will be restored once StarRocks' thirdparty OpenSSL is upgraded to 3.x, at which point the in-process ABI will match what `libwfssl.so` expects.
2. **Documentation**: this FAQ and related guidance for configuring `core-site.xml`.

> Note: an earlier attempt to install `libssl-dev` inside the Ubuntu runtime Docker image ([PR #70688](https://github.com/StarRocks/starrocks/pull/70688)) has been superseded. The crash is caused by `libwfssl.so`'s OpenSSL 3.x call sites dispatching into the **static OpenSSL 1.x embedded in the `starrocks_be` binary**, not by a missing host-side OpenSSL package -- so installing `libssl-dev` / `openssl-devel` cannot change the outcome.

---

## Quick Reference: Minimal Fix

Pick **either** of the following on **every CN/BE node** (and every Broker node, if deployed), then restart the service. Do NOT rely on installing `libssl-dev` or `openssl-devel` -- those packages do not fix this crash.

**Option A -- delete the `wildfly-openssl` JAR:**

```bash
rm -f $STARROCKS_HOME/lib/hadoop/common/wildfly-openssl-2.2.5.Final.jar
```

**Option B -- force JSSE via `$STARROCKS_HOME/conf/core-site.xml`:**

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

## References

- [HADOOP-19719: Upgrade to wildfly version with support for openssl 3](https://issues.apache.org/jira/browse/HADOOP-19719)
- [HADOOP-19262: Upgrade wildfly-openssl for JDK 17+](https://issues.apache.org/jira/browse/HADOOP-19262)
- [FLINK-38284: Flink downstream tracking issue](https://issues.apache.org/jira/browse/FLINK-38284)
- [Hadoop S3A Troubleshooting Guide](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/troubleshooting_s3a.html)
- [Hadoop S3A Performance Guide (SSL Channel Mode)](https://apache.github.io/hadoop/hadoop-aws/tools/hadoop-aws/performance.html)
- [Wildfly OpenSSL GitHub Repository](https://github.com/wildfly-security/wildfly-openssl)
- [StarRocks Issue #70478: SIGSEGV on Azure ADLS2 query (4.0.7)](https://github.com/StarRocks/starrocks/issues/70478)
- [StarRocks PR #69503: Upgrade Hadoop 3.4.2 to 3.4.3](https://github.com/StarRocks/starrocks/pull/69503)
- [StarRocks Issue #71898: Drop `wildfly-openssl` from the BE package](https://github.com/StarRocks/starrocks/issues/71898)
- [StarRocks PR #70688: Install libssl-dev for Ubuntu runtime (superseded, does not fix the crash)](https://github.com/StarRocks/starrocks/pull/70688)
