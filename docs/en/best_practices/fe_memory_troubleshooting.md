---
displayed_sidebar: docs
keywords: ['FE', 'Coordinator Node','memory', 'OOM', 'JVM', 'troubleshooting', 'heap']
---

# FE / Coordinator Node memory full issue troubleshooting

Prevent "FE memory full" issues, quickly recover, and find out the cause as much as possible to prevent recurrence.

:::note

- This information is applicable to FE nodes and Coordinator nodes (shared-data deployments). For readability, we will use "FE" to refer to both types of frontend nodes.
- This article mainly analyzes the situation and solutions of in-heap memory, without covering off-heap memory. If the OOM is caused by off-heap memory issues, consider reducing the JVM XMX configuration or expanding the machine's memory.

:::

## FE Memory Structure and Allocation Recommendations

### FE Memory Composition

The front-end service (FE) runs on the JVM, and its memory consumption is from the Java heap and from off-heap memory overhead.

#### Overview of JVM Memory Modules

All memory allocated through the JVM during the FE process can be roughly divided into the following modules:

| Module | Type | Description |
|:--------|-------------|:------|
| **Java Heap** | Heap | Memory of Java object instances, such as Plan Cache, metadata, temporary objects, etc. |
| **Class** | Off-heap | Structures such as metadata generated when loading a Class |
| **Thread** | Off-heap | The memory occupied by threads is mainly the stack space of each thread (usually about 1MB per thread). |
| **Code** | Off-heap | Memory occupied by JVM when compiling bytecode into native machine code |
| **GC** | Off-heap | Internal working data structures of the garbage collector |
| **Compiler** | Off-heap | The memory used by the HotSpot compiler may be similar to that of Code. |
| **Other** | Off-heap | Including direct memory such as Direct ByteBuffer, Netty buffer, etc. |
| **Symbol** | Off-heap | SymbolTable structures such as string constant pool, integer constant pool, etc. |
| **Native Memory Tracking (NMT)** | Off-heap | NMT's own recorded memory usage occupies memory |
| **Arena Chunk** | Off-heap | Temporary memory blocks (allocated using malloc) used internally by the JVM for temporary storage and reuse |
| **Logging** | Off-heap | Memory occupied by logging systems (such as log4j) |
| **Arguments** | Off-heap | Memory occupied by JVM startup parameter passing |
| **Module** | Off-heap | Structures used in the Java Module System |

Considering the operating characteristics of JVM and StarRocks, the memory of FE mainly consists of the following parts:

1. **JVM Heap Memory (Heap)**
   - Stores Java object instances, such as Plan Cache, metadata cache, etc.
   - The maximum size is set via the parameter `-Xmx` and is significantly affected by the garbage collection mechanism.

2. **Non-heap Memory (Metaspace)**
   - Metadata such as storage class definitions, constant pool, etc.
   - Java 8 and later replaced PermGen.

3. **Direct Memory**
   - Off-heap memory used by Netty, ByteBuffer, log buffer, RPC framework, etc.
   - Allocated by `sun.misc.Unsafe`, usually not controlled by GC.

4. **Thread Stack Memory**
   - Each Java thread will request a thread stack when created, with a default size of approximately 1MB (configurable via `-Xss`).
   - When the number of threads is large (such as in high-concurrency scenarios), it is easy to cause memory backlog.

5. **JNI / Native Memory Call**
   - The database may consume memory when calling the native layer through logging components (such as RocksDB, Netty), Cache, etc.

### JVM Parameter Configuration Recommendations

In the JVM, the heap memory is configured via `Xmx`. Generally, it is reasonable for the memory used by the process to be less than 130% of `Xmx`. That is to say, in addition to the memory configured by JVM `Xmx`, a portion of off-heap memory will also be used. For example, if JVM is configured with 21g, the RSS memory usage of the process seen in `top` will be approximately 27g.

Based on experience, the recommended relationship between the number of tablets and FE memory is:

| Tablet Count | Recommended FE Memory Configuration |
|:-------------|:------------------------------------|
| Less than 1M | 16 GB                               |
| 1M - 2M      | 32 GB                               |
| 2M - 5M      | 64 GB                               |
| 5M - 10M     | 128 GB                              |

For independently deployed environments, it is recommended to reserve sufficient memory for the system. In addition to the memory reserved for the system, off-heap memory also needs to be considered.

For hybrid deployment environments, it is necessary to reserve sufficient memory as much as possible based on the usage considerations of other services to prevent OOM.

In the case of independent deployment of FE (mixed deployment is not recommended):

1. When the machine memory is less than 32G, `-Xmx` (Max Heap Size) should be configured to a maximum of 70% of the machine memory.
2. When the machine memory is greater than 32G, `-Xmx` (Max Heap Size) should be configured to a maximum of 80% of the machine memory.
3. Since the FE leader node needs to perform checkpoints, in general, the memory of the leader node will be about twice that of the follower node.

In version 3.4+, the leader will select a follower node to perform the checkpoint, then download the follower's results and distribute them to other followers. The leader's memory usage will be close to that of the followers. See [PR #52103](https://github.com/StarRocks/starrocks/pull/52103).

### FE JVM Monitoring and Memory Profile Deployment

For FE state, heap monitoring and GC monitoring can effectively monitor the state of the service and are also key clues for troubleshooting issues afterwards. It is recommended to configure monitoring in a timely manner, and configure corresponding alerts if necessary.

The specific monitoring indicators are as follows:

| Indicator                  | Description                             |
|:---------------------------|:----------------------------------------|
| `jvm_heap_size_bytes`      | Heap memory usage.                      |
| `jvm_non_heap_size_bytes`  | Mainly includes metaspace, code cache, etc., does not include all off-heap memory. |
| `jvm_old_gc{type="count"}` | Full GC count.                          |
| `jvm_old_gc{type="time"}`  | Full GC time.                           |

You can also view the monitoring dashboard **FE JVM** in Grafana or Manager.

Memory profile can analyze sudden heap increases. It is necessary to confirm whether it works properly after the service installation is completed.

In 3.3.6+, the database will periodically print the memory profile and output it to `log/proc_profile`, in HTML format, compressed via tgz.

Control configuration:

```properties
proc_profile_collect_interval_s = 600
proc_profile_collect_time_s = 300
proc_profile_cpu_enable = true
proc_profile_mem_enable = true
```

For versions prior to 3.3.6, you can periodically print the profile via a script. Create a new script file named `mem_profiler.sh` under the installation directory of each FE and copy the following content into the file.

:::note
This script has no impact on the FE process, does not affect the normal use of the service, and will automatically clean up files that have expired for 48 hours.
:::

```bash
#!/bin/bash
mkdir -p mem_alloc_log

cleanup_old_files() {
    find mem_alloc_log -name "alloc-profile-*.html" -mmin +2880 -exec rm -f {} \;
}

while true
do
    cleanup_old_files

    current_time=$(date +'%Y-%m-%d-%H-%M-%S')
    file_name="mem_alloc_log/alloc-profile-${current_time}.html"
    ./bin/profiler.sh -e alloc --alloc 2m -d 300 -f "$file_name" `cat bin/fe.pid`
done
```

```bash
# Background startup
chmod +x mem_profiler.sh
nohup ./mem_profiler.sh > mem_profiler.out 2>&1 &

# Check if the process exists
ps aux | grep mem_profiler.sh

# Stop the process
pkill -f mem_profiler.sh
```

## FE Crash

The causes of FE crashes are roughly divided into 4 types.

### 1. The FE process has high memory usage and was killed by the operating system

You can check whether this is the cause by querying the system logs:

```bash
# Check dmesg
dmesg | grep -iE 'out of memory|oom|kill|killed process'

# View message log
sudo grep -Ei 'killed process|oom.kill|sending SIG' -C5 /var/log/messages /var/log/syslog 2>/dev/null | tail -n 100
```

After being OOM killed, the log may print the memory usage of system processes. Fields such as `total_vm` and `rss` displayed by OOM are in units of pages, with the default being 1 page = 4KB:

- `total_vm = 8793525` → indicates that it has requested approximately 8793525 × 4KB ≈ 33.5 GB of virtual memory
- `rss = 7218120` → indicates that approximately 27.5 GB of physical memory has been actually used, which is 7218120 × 4KB

Example OOM log:

```
Jun 10 11:20:24 tp-prod-bigdata-sr-ss-fe-2-b kernel: [ pid ]  uid  tgid  total_vm     rss  nr_ptes  swapents  oom_score_adj  name
Jun 10 11:21:58 tp-prod-bigdata-sr-ss-fe-2-b kernel: [22654]  1005 22654   8793525 7218120    14943         0              0  java
```

Check the actual RSS usage. If it is very close to the machine's memory, it is generally due to unreasonable JVM allocation. Refer to the [JVM Parameter Configuration Recommendations](#jvm-parameter-configuration-recommendations) section.

### 2. The FE leader heap memory is high, triggering a Full GC, leading to a leader switch

When the old leader exits due to a leader switch, the following information is printed in `fe.out`:

```
transfer FE type from LEADER to UNKNOWN. exit
```
or
```
transfer FE type from LEADER to FOLLOWER. exit
```

Most leader switches are caused by Full GC; in a few cases, they are triggered by high leader CPU usage or the execution of `jstack`.

**Analyzing GC logs:**

Upload the GC log to [https://gceasy.io/](https://gceasy.io/) and click **Pause GC Duration**. If the duration of a single pause GC exceeds 30 seconds, or if there are frequent pause GCs exceeding 10 seconds, it will trigger a leader switch.

Click **Heap before GC** to find the time point when heap memory rises. Use this time range to locate the memory profile file for that period, which will identify which module was requesting memory at that time.

### 3. An internal bug in the FE program caused a crash

This situation is not within the scope of this discussion. Refer to the [Restore Metadata](../administration/Meta_recovery.md) documentation.

### 4. JVM bug causes FE crash

This situation is very rare. After encountering it, the cause can be investigated by checking the crash log, located at `log/hs_err_pid%p.log`, where `%p` is the process ID.

## FE Process Is Normal But Memory Is High

During the long-term operation of the FE, the in-heap memory usage may abnormally increase, frequently triggering Full GC, which causes queries or imports to slow down. The problem may manifest as heap **sudden increase** or continuous **slow growth**.

### Process of On-site Investigation

Log in to the FE monitoring section corresponding to Grafana or Manager.

**Step 1: Check the following monitoring**

- **FE JVM Panel**: JVM Heap Memory Used metric

**Step 2: Identify the type of memory increase**

| Type | Feature | Subsequent Troubleshooting Path |
|------|---------|--------------------------------|
| **Sudden increase** | Heap memory spikes sharply within a short period (seconds to minutes) | Prioritize looking at QPS, number of connections, and SQL requests |
| **Slowly rises** | Memory steadily increases, and post-GC recovery deteriorates. | Prioritize troubleshooting memory leaks or cache accumulation |

### Memory Spike Investigation

Whether the request pressure increases within a short period — high QPS or highly concurrent metadata operations can lead to a surge in memory.

**1. Check the following monitoring:**

- **QPS Panel**: Has the number of requests received by FE suddenly increased?
- **Connections Panel**: Does the number of concurrent connections of FE spike instantaneously?

**2. Confirm abnormal SQL through audit tables or logs:**

Query high-frequency SQL statements in the last 5-10 minutes:

```sql
SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__
WHERE feip = 'IP of memory abnormal node'
  AND `timestamp` >= now() - INTERVAL 1000 MINUTE
ORDER BY `timestamp` DESC;
```

Pay special attention to:

- Statements with deep subqueries, multiple JOINs, and complex aggregations
- Unconventional business SQL (such as concurrent SQL for datasets generated by the BI platform)
- In version 3.3.13+, you can check the FE audit log for the `queryFeMemory` field, which indicates how much memory a query has applied for in total in the FE.
- When the FE process exits due to FE OOM, it will automatically print the query being executed and the corresponding `QueryFEAllocatedMemory`.

Search keyword in FE log:

```
QueryFEAllocatedMemory
```

**3. Check through the FE log for a large number of metadata operations (table creation and deletion):**

```bash
# Create a table
grep "Begin to unprotect create table" fe.log

# Drop a table
grep "Finished drop table" fe.log
```

### Memory Slowly Increasing

**4. The memory configured for the JVM is too small**

Monitor the GC status of the FE process to determine whether Full GC occurs frequently. Check the JVM memory usage through the `jstat` command:

```bash
jstat -gcutil $pid 1000 1000
```

Example output:

```
 S0     S1     E      O      M     CCS    YGC     YGCT    FGC    FGCT     GCT
  0.00 100.00  27.78  95.45  97.77  94.45   24    0.226     1    0.065    0.291
  0.00 100.00  44.44  95.45  97.77  94.45   24    0.226     1    0.065    0.291
  0.00 100.00  55.56  95.45  97.77  94.45   24    0.226     1    0.065    0.291
```

If the `O` (Old Generation) percentage has been relatively high, it indicates a problem with the JVM memory configuration, and the JVM memory needs to be increased.

**5. Observe whether there is a leak through the FE's memory tracker (3.3.7+)**

You can locate which module has a memory leak by checking the logs of `MemoryUsageTracker`, which prints the memory consumption of each module once every hour:

```
2025-06-06 16:37:50.633+08:00 INFO (MemoryUsageTracker|74)
[MemoryUsageTracker.trackMemory():161] (6ms) Module Dict -
CacheDictManager estimated 0B of memory. Contains ColumnDict with 0 object(s).

2025-06-06 16:37:50.657+08:00 INFO (MemoryUsageTracker|74)
[MemoryUsageTracker.trackMemory():161] (21ms) Module LocalMetastore -
LocalMetastore estimated 3.8MB of memory. Contains Partition with 17473 object(s).

2025-06-06 16:37:50.667+08:00 INFO (MemoryUsageTracker|74)
[MemoryUsageTracker.trackMemory():161] (0ms) Module TabletInvertedIndex -
TabletInvertedIndex estimated 37.7MB of memory. Contains TabletMeta with 353459 object(s).

2025-06-06 16:37:50.741+08:00 INFO (MemoryUsageTracker|74)
[MemoryUsageTracker.trackMemory():108] total tracked memory: 46.8MB,
jvm: Process used: 18.6GB, heap used: 4.4GB, non heap used: 289.1MB, direct buffer used: 395.5MB
```

Compare the memory growth of each module across recent log entries to identify which module is growing.

### On-site Information Collection

**If the process has not been restarted yet, collect `jmap` information immediately.**

#### jmap Memory Troubleshooting Process

**Step 1: Confirm the process PID of FE**

```bash
ps aux | grep FE
```

**Step 2: Use jmap**

:::warning[Precautions]
- `jmap -dump` will pause the FE process for a short period of time. Use with caution when high online stability is required, and it is recommended to notify in advance.
- `jmap -histo` generally has no impact on the process; GC triggering may take dozens of milliseconds.
- Frequent use of `-dump` to export heap files is not recommended, as it incurs significant overhead.
- `histo` is sufficient for preliminary judgment of large objects and does not necessarily require a dump.
:::

**`jmap -histo:live` (Use with caution in production environments)**

Captures the current in-heap object distribution by forcing a GC. Using the `live` parameter may resolve high memory usage issues:

```bash
jmap -histo:live <pid> > jmap_histo_$(date +%s).log
```

It is recommended to take two samples and compare the differences:

```bash
jmap -histo:live <pid> > histo_1.log
sleep 60
jmap -histo:live <pid> > histo_2.log
```

**`jmap -histo pid` (lightweight, does not trigger Full GC)**

```bash
# Retrieves all objects within the current JVM heap, which may include
# objects that have been cleaned up — will affect result analysis.
jmap -histo <pid> > histo.txt
```

**Step 3: Analyze Top N Object Occupancy**

View the top entries of the file:

```bash
head -n 30 histo_2.log
```

Field explanation:

| Column     | Meaning        |
|------------|----------------|
| num        | Class number   |
| #instances | Instance count |
| #bytes     | Total bytes    |
| class name | Object name    |

Compare `histo_1` and `histo_2` to identify which classes have significantly increased instances or memory.

Example: Classes starting with `java` are utility classes referenced by business classes. Looking from top to bottom, related classes at the front account for a larger proportion. For example, `com.starrocks.lake.LakeTablet` occupying significant memory indicates excessive tablet usage.

**Step 4: Export a complete heap dump (if histo is insufficient)**

```bash
# Will trigger Full GC. Use with caution.
jmap -dump:live,format=b,file=heap_$(date +%s).hprof <pid>
```

This file may be very large (several GB) and needs to be opened and analyzed using **MAT** or **VisualVM**.

**Step 5: Configure automatic dump on OOM**

Add the following to the JVM configuration in `fe.conf` to automatically generate a dump file when FE runs out of memory:

```bash
JAVA_OPTS="-Dlog4j2.formatMsgNoLookups=true \
  -Xmx8192m \
  -XX:+UseG1GC \
  -Xlog:gc*:${LOG_DIR}/fe.gc.log.$DATE:time \
  -XX:ErrorFile=${LOG_DIR}/hs_err_pid%p.log \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=${LOG_DIR}/heap_dump_oom.hprof \
  -Djava.security.policy=${STARROCKS_HOME}/conf/udf_security.policy \
  -Djava.security.krb5.conf=/etc/krb5.conf \
  -Dsun.security.krb5.debug=true \
  -Dsun.security.spnego.debug=true \
  -Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:8113"
```

Note: `HeapDumpPath` specifies a directory path, not a file name.

#### Keep Memory Profile Files for Analysis

Starting from version 3.3.6, memory profiles are output to the `log/proc_profile` directory, in HTML format, compressed using tgz.

Find the file corresponding to the time of memory increase, decompress it, and open it in a browser to see the flame graph of memory allocation. The wider part of the stack represents more memory allocation.

If no memory profile is printed during the period of memory increase, you can turn off the CPU profile and adjust the printing interval to 5 minutes:

```properties
proc_profile_cpu_enable = false
proc_profile_collect_interval_s = 300
```

These are dynamic parameters that can be modified directly without restart:

```sql
ADMIN SET FRONTEND CONFIG ("proc_profile_cpu_enable" = "false");
ADMIN SET FRONTEND CONFIG ("proc_profile_collect_interval_s" = "300");
```

## Emergency Recovery

**1. Restart the service**

After stopping the service via Manager, click Start again.

If the Manager page is inaccessible, or clicking Restart has no response, log in to the machine where the FE service is located and restart it using the `agentctl.sh` script:

```bash
./agentctl.sh

# Example output:
# agent-service                                        RUNNING   pid 1657, uptime 0:45:21
# fe-202507111704-81ea3677000043bc867368c9bd8628ec     RUNNING   pid 1659, uptime 0:45:21

supervisor> restart fe-202507111704-81ea3677000043bc867368c9bd8628ec
# fe-202507111704-81ea3677000043bc867368c9bd8628ec: stopped
# fe-202507111704-81ea3677000043bc867368c9bd8628ec: started

supervisor> status
```

**2. Adjust the heap memory configuration to a larger value and restart**

Modify the `-Xmx` value in `fe.conf`:

```bash
JAVA_OPTS="-Dlog4j2.formatMsgNoLookups=true -Xmx8192m -XX:+UseG1GC \
  -Xlog:gc*:${LOG_DIR}/fe.gc.log.$DATE:time \
  -XX:ErrorFile=${LOG_DIR}/hs_err_pid%p.log \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=${LOG_DIR}/heap_dump_oom.hprof \
  -Djava.security.policy=${STARROCKS_HOME}/conf/udf_security.policy \
  -Djava.security.krb5.conf=/etc/krb5.conf \
  -Dsun.security.krb5.debug=true -Dsun.security.spnego.debug=true \
  -Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:8113"
```

**3. If the machine itself has limited memory**, restarting may still result in OOM issues. Consider expanding the machine's memory, then increasing the JVM configuration before restarting.

**4. If a historically corresponding PR is found in the case**, it is recommended to upgrade to the corresponding fixed version.

## Case Collection

### Case 1: Frequent and large-scale metadata operations cause high FE memory usage

**Background**: A client's business requires regular table creation and cleanup, with sequential operations creating dozens of tables per second and synchronous operations deleting them, resulting in the creation of hundreds of thousands of tables throughout the process.

**Problem manifestation**: Heap memory and machine CPU continue to rise, and query response slows down. After checking the FE logs, a large number of table creation and deletion operations were found, which led to a sharp increase in service pressure.

**Recovery Method**: Stop the table creation and deletion jobs, restart the service, and resume normal use.

**Solution**: For this business scenario, when using `DROP TABLE`, `FORCE` must be added; otherwise, the metadata will continue to be occupied. At the same time, reduce the concurrency frequency of table creation.

---

### Case 2: Complex concurrent SQL causes a sudden increase in FE memory

**Problem manifestation**: Heap memory suddenly increased, and scheduled import tasks were significantly delayed. QPS and the number of connections were within the normal range of variation. Review of audit logs and FE logs revealed many highly complex queries in concurrent scheduled SQL, which resulted in a large amount of memory being occupied during query plan parsing.

**Recovery Method**: Expand the memory of the FE service — stop the machine to expand its memory, increase the JVM XMX configuration, and restart the service.

**Solution**: Split complex SQL, observe the memory usage during peak job periods, and ensure it stays below 80%.

---

### Case 3: MV refresh is too frequent, causing the leader FE memory to grow slowly and remain high

**Background**: The user has MV refresh tasks, and many tasks are configured for minute or second-level refresh.

**Problem manifestation**: FE leader memory continues to rise, and queries are laggy. Checking the FE log reveals many slow locks, causing occupied resources to fail to be released.

Example slow lock log:

```
2025-06-06 15:41:43.813+08:00 WARN (AutoStatistic|41)
[LockManager.logSlowLockTrace():423] LockManager detects slow lock :
{"owners":[{"id":13479085,"name":"starrocks-taskrun-pool-22075","type":"INTENTION_SHARED","heldFor":6935,"queryId":"ab976f93-42a9-11f0-98e6-fa163e3710f8","waitTime":0,"stack":["java.base@11.0.20.1/java.net.SocketInputStream.socketRead0(Native Method)","java.base@11.0.20.1/java.net.SocketInputStream.socketRead(SocketInputStream.java:115)","java.base@11.0.20.1/java.net.SocketInputStream.read(SocketInputStream.java:168)","java.base@11.0.20.1/java.net.SocketInputStream.read(SocketInputStream.java:140)","app//org.postgresql.core.VisibleBufferedInputStream.readMore(VisibleBufferedInputStream.java:161)","app//org.postgresql.core.VisibleBufferedInputStream.ensureBytes(VisibleBufferedInputStream.java:128)","app//org.postgresql.core.VisibleBufferedInputStream.ensureBytes(VisibleBufferedInputStream.java:113)","app//org.postgresql.core.VisibleBufferedInputStream.read(VisibleBufferedInputStream.java:73)","app//org.postgresql.core.PGStream.receiveChar(PGStream.java:453)","app//org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:2120)","app//org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:356)","app//org.postgresql.jdbc.PgStatement.executeInternal(PgStatement.java:496)","app//org.postgresql.jdbc.PgStatement.execute(PgStatement.java:413)","app//org.postgresql.jdbc.PgStatement.executeWithFlags(PgStatement.java:333)","app//org.postgresql.jdbc.PgStatement.executeCachedSql(PgStatement.java:319)","app//org.postgresql.jdbc.PgStatement.executeWithFlags(PgStatement.java:295)","app//org.postgresql.jdbc.PgStatement.executeQuery(PgStatement.java:244)","app//org.postgresql.jdbc.PgDatabaseMetaData.getColumns(PgDatabaseMetaData.java:1584)","app//com.starrocks.connector.jdbc.PostgresSchemaResolver.getColumns(PostgresSchemaResolver.java:49)","app//com.starrocks.connector.jdbc.JDBCMetadata.lambda$getTable$1(JDBCMetadata.java:200)","app//com.starrocks.connector.jdbc.JDBCMetadata$$Lambda$1340/0x000014e17aa20960.apply(Unknown Source)","app//com.starrocks.connector.jdbc.JDBCMetaCache.get(JDBCMetaCache.java:73)","app//com.starrocks.connector.jdbc.JDBCMetadata.getTable(JDBCMetadata.java:197)","app//com.starrocks.connector.CatalogConnectorMetadata.getTable(CatalogConnectorMetadata.java:136)","app//com.starrocks.server.MetadataMgr.lambda$getTable$5(MetadataMgr.java:501)","app//com.starrocks.server.MetadataMgr$$Lambda$513/0x000014e2116f0cb0.apply(Unknown Source)","java.base@11.0.20.1/java.util.Optional.map(Optional.java:265)","app//com.starrocks.server.MetadataMgr.getTable(MetadataMgr.java:501)","app//com.starrocks.sql.analyzer.QueryAnalyzer.resolveTable(QueryAnalyzer.java:1391)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.resolveTableRef(QueryAnalyzer.java:482)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.resolveTableRef(QueryAnalyzer.java:420)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.resolveTableRef(QueryAnalyzer.java:420)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSelect(QueryAnalyzer.java:363)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSelect(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.SelectRelation.accept(SelectRelation.java:232)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryRelation(QueryAnalyzer.java:303)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryStatement(QueryAnalyzer.java:293)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryStatement(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.QueryStatement.accept(QueryStatement.java:70)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSubquery(QueryAnalyzer.java:907)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSubquery(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.SubqueryRelation.accept(SubqueryRelation.java:66)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSelect(QueryAnalyzer.java:370)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSelect(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.SelectRelation.accept(SelectRelation.java:232)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryRelation(QueryAnalyzer.java:303)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryStatement(QueryAnalyzer.java:293)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryStatement(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.QueryStatement.accept(QueryStatement.java:70)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSubquery(QueryAnalyzer.java:907)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSubquery(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.SubqueryRelation.accept(SubqueryRelation.java:66)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitJoin(QueryAnalyzer.java:739)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitJoin(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.JoinRelation.accept(JoinRelation.java:134)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSelect(QueryAnalyzer.java:370)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSelect(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.SelectRelation.accept(SelectRelation.java:232)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryRelation(QueryAnalyzer.java:303)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryStatement(QueryAnalyzer.java:293)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryStatement(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.QueryStatement.accept(QueryStatement.java:70)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer.analyze(QueryAnalyzer.java:121)","app//com.starrocks.sql.analyzer.InsertAnalyzer.analyzeWithDeferredLock(InsertAnalyzer.java:74)","app//com.starrocks.sql.analyzer.InsertAnalyzer.analyze(InsertAnalyzer.java:62)","app//com.starrocks.sql.analyzer.Analyzer$AnalyzerVisitor.visitInsertStatement(Analyzer.java:398)","app//com.starrocks.sql.analyzer.Analyzer$AnalyzerVisitor.visitInsertStatement(Analyzer.java:176)","app//com.starrocks.sql.ast.InsertStmt.accept(InsertStmt.java:304)","app//com.starrocks.sql.ast.AstVisitor.visit(AstVisitor.java:80)","app//com.starrocks.sql.analyzer.Analyzer.analyze(Analyzer.java:173)","app//com.starrocks.sql.StatementPlanner.analyzeStatement(StatementPlanner.java:218)","app//com.starrocks.sql.StatementPlanner.plan(StatementPlanner.java:116)","app//com.starrocks.sql.StatementPlanner.plan(StatementPlanner.java:95)","app//com.starrocks.load.InsertOverwriteJobRunner.executeInsert(InsertOverwriteJobRunner.java:351)","app//com.starrocks.load.InsertOverwriteJobRunner.doLoad(InsertOverwriteJobRunner.java:171)","app//com.starrocks.load.InsertOverwriteJobRunner.handle(InsertOverwriteJobRunner.java:151)","app//com.starrocks.load.InsertOverwriteJobRunner.transferTo(InsertOverwriteJobRunner.java:212)","app//com.starrocks.load.InsertOverwriteJobRunner.prepare(InsertOverwriteJobRunner.java:256)","app//com.starrocks.load.InsertOverwriteJobRunner.handle(InsertOverwriteJobRunner.java:148)","app//com.starrocks.load.InsertOverwriteJobRunner.run(InsertOverwriteJobRunner.java:136)","app//com.starrocks.load.InsertOverwriteJobMgr.executeJob(InsertOverwriteJobMgr.java:91)","app//com.starrocks.qe.StmtExecutor.handleInsertOverwrite(StmtExecutor.java:2152)","app//com.starrocks.qe.StmtExecutor.handleDMLStmt(StmtExecutor.java:2244)","app//com.starrocks.qe.StmtExecutor.handleDMLStmtWithProfile(StmtExecutor.java:2161)","app//com.starrocks.scheduler.PartitionBasedMvRefreshProcessor.refreshMaterializedView(PartitionBasedMvRefreshProcessor.java:1271)","app//com.starrocks.scheduler.PartitionBasedMvRefreshProcessor.doRefreshMaterializedView(PartitionBasedMvRefreshProcessor.java:464)","app//com.starrocks.scheduler.PartitionBasedMvRefreshProcessor.doRefreshMaterializedViewWithRetry(PartitionBasedMvRefreshProcessor.java:373)","app//com.starrocks.scheduler.PartitionBasedMvRefreshProcessor.doMvRefresh(PartitionBasedMvRefreshProcessor.java:332)","app//com.starrocks.scheduler.PartitionBasedMvRefreshProcessor.processTaskRun(PartitionBasedMvRefreshProcessor.java:200)","app//com.starrocks.scheduler.TaskRun.executeTaskRun(TaskRun.java:285)","app//com.starrocks.scheduler.TaskRunExecutor.lambda$executeTaskRun$0(TaskRunExecutor.java:60)","app//com.starrocks.scheduler.TaskRunExecutor$$Lambda$3311/0x000014db15a06cb0.get(Unknown Source)","java.base@11.0.20.1/java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1700)","java.base@11.0.20.1/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)","java.base@11.0.20.1/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)","java.base@11.0.20.1/java.lang.Thread.run(Thread.java:829)"]}],"waiter":[{"id":13446845,"name":"thrift-server-pool-367701","type":"WRITE","waitTime":6895},{"id":5268046,"name":"lake-publish-task-8973","type":"INTENTION_EXCLUSIVE","waitTime":6895},{"id":5268447,"name":"lake-publish-task-9181","type":"INTENTION_EXCLUSIVE","waitTime":6874},{"id":5267898,"name":"lake-publish-task-8911","type":"INTENTION_EXCLUSIVE","waitTime":6874},{"id":5267726,"name":"lake-publish-task-8827","type":"INTENTION_EXCLUSIVE","waitTime":6874},{"id":13494208,"name":"thrift-server-pool-369089","type":"INTENTION_EXCLUSIVE","waitTime":6857},{"id":13492395,"name":"starrocks-taskrun-pool-22096","type":"INTENTION_EXCLUSIVE","waitTime":6854},{"id":41,"name":"AutoStatistic","type":"INTENTION_SHARED","waitTime":6854,"queryId":"ad195549-42a9-11f0-98e6-fa163e3710f8"},{"id":486,"name":"nioEventLoopGroup-6-33","type":"INTENTION_SHARED","waitTime":6852},{"id":5267924,"name":"lake-publish-task-8925","type":"INTENTION_SHARED","waitTime":6849},{"id":5271934,"name":"lake-publish-task-9265","type":"INTENTION_SHARED","waitTime":6849},{"id":5344442,"name":"lake-publish-task-9312","type":"INTENTION_EXCLUSIVE","waitTime":6786},{"id":13471238,"name":"thrift-server-pool-368401","type":"INTENTION_SHARED","waitTime":55},{"id":13494164,"name":"thrift-server-pool-369065","type":"INTENTION_SHARED","waitTime":55},{"id":13481141,"name":"thrift-server-pool-368676","type":"INTENTION_SHARED","waitTime":54}]}
```

**Recovery Method**: After leader GC, it switches to the primary and self-recovers.

**Solution**: Relax the refresh frequency. Adjust non-real-time strongly dependent scenarios to hourly or even daily levels to avoid a large number of minute-level MVs being triggered simultaneously. Once slow locks disappear, the problem is resolved.

---

### Case 4: Querying external tables in the Iceberg catalog causes an increase in FE memory

**Background**: User executes a query job on an Iceberg table.

**Problem manifestation**: After submitting the SQL, the memory of the FE leader increased significantly, and CPU also continued to rise. Other query and import tasks reported errors. Checking the process status revealed that frequent GC was triggered, causing abnormal service status.

**Recovery Method**: Restart the leader node and regularly manage the delete files of the corresponding Iceberg table.

---

### Case 5: Insert memory leak issue causes abnormal increase in FE memory

**Background**: During user operation, it was found that the FE memory abnormally increased, the process restarted, and the leader was not properly switched. The main task of the client cluster was import tasks.

**Problem manifestation**: Export `jmap` for the leader node process for troubleshooting:

```bash
jmap -histo <FEpid> > jmap.txt  # This operation will not trigger Full GC
```

Found in the exported file:

```bash
14: 521358 154321968 com.starrocks.load.loadv2.InsertLoadJob
```

The number of `InsertLoadJob` instances is 520,000. When this number exceeds 10,000, it indicates that a memory leak has occurred.

- [Issue](https://github.com/StarRocks/starrocks/issues/538100
- [Fix PR](https://github.com/StarRocks/starrocks/pull/53809)

**Affected versions**:

- 3.1.0 - 3.1.16
- 3.2.0 - 3.2.12
- 3.3.0 - 3.3.7

**Fixed versions**: 3.1.17+, 3.2.13+, 3.3.8+

**Recovery Method**: Upgrade the cluster version.

---

### Case 6: async-profiler causes JVM crash, leading to FE crash

**Problem manifestation**: FE process crashed or restarted.

**Problem Troubleshooting**: When the process is abnormal and the file `hs_err_pid$pid.log` is generated in the log directory, the FE crash may be caused by a JVM crash.

```
--------------- S U M M A R Y ------------
...
--------------- T H R E A D ---------------
Current thread: JavaThread "tablet scheduler" ...

Native frames:
V  [libjvm.so] frame::entry_frame_is_first() const
V  [libjvm.so] forte_fill_call_trace_given_top(...)
V  [libjvm.so] AsyncGetCallTrace
C  [libasyncProfiler.so] Profiler::getJavaTraceAsync(...)
C  [libasyncProfiler.so] Profiler::recordSample(...)
C  [libasyncProfiler.so] PerfEvents::signalHandler(...)
```

The crash breakpoint `PerfEvents::signalHandler` is caused by `async-profiler`. The tool triggered a segmentation fault inside the JVM while attempting to obtain the Java call stack. In the enterprise version, this tool is deployed by default to periodically obtain process information.

**Processing Method**: Stop `async-profiler` CPU information collection:

```sql
ADMIN SET FRONTEND CONFIG ("proc_profile_cpu_enable" = "false");
```

Also add the following to `fe.conf`:

```properties
proc_profile_cpu_enable = false
```

---

### Case 7: OOM caused by insufficient JVM heap configuration

Under normal circumstances, with 3 FEs, if the leader node crashes due to insufficient memory, the remaining 2 FEs can elect a new leader and provide services. If the 2 follower nodes cannot communicate properly after the leader crashes, the FE that is about to be promoted to leader may also exit due to insufficient metadata replicas, resulting in 2 FEs crashing.

**Problem Troubleshooting**:

FE leader log shows:

```
java.lang.OutOfMemoryError: Java heap space
2025-08-09 08:22:30.926-06:00 WARN (thrift-server-pool-9443633|65019425)
[TIOStreamTransport.close():153] Error closing output stream.
java.net.SocketException: Socket closed
    at java.net.SocketOutputStream.socketWrite(SocketOutputStream.java:113) ~[?:?]
    at java.net.SocketOutputStream.write(SocketOutputStream.java:150) ~[?:?]
    at java.io.BufferedOutputStream.flushBuffer(BufferedOutputStream.java:81) ~[?:?]
    at java.io.BufferedOutputStream.flush(BufferedOutputStream.java:142) ~[?:?]
    at java.io.FilterOutputStream.close(FilterOutputStream.java:182) ~[?:?]
    at org.apache.thrift.transport.TIOStreamTransport.close(TIOStreamTransport.java:151) ~[libthrift-0.20.0.jar:0.20.0]
    at org.apache.thrift.transport.TSocket.close(TSocket.java:238) ~[libthrift-0.20.0.jar:0.20.0]
    at com.starrocks.common.SRTThreadPoolServer$WorkerProcess.run(SRTThreadPoolServer.java:326) ~[starrocks-fe.jar:?]
    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128) ~[?:?]
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628) ~[?:?]
    at java.lang.Thread.run(Thread.java:829) ~[?:?]
2025-08-09 08:22:30.927-06:00 ERROR (thrift-server-accept|144)
[SRTThreadPoolServer.execute():221] ExecutorService threw error:
java.lang.OutOfMemoryError: Java heap space
java.lang.OutOfMemoryError: Java heap space
2025-08-09 08:22:30.931-06:00 WARN (starrocks-mysql-nio-pool-42252|65019486)
[StmtExecutor.execute():595] New planner error: ...
com.starrocks.sql.common.StarRocksPlannerException: StarRocks planner use long time 3000 ms in memo phase,
This probably because 1. FE Full GC, 2. Hive external table fetch metadata took a long time, 3. The SQL is
very complex. You could 1. adjust FE JVM config, 2. try query again, 3. enlarge new_planner_optimize_timeout
session variable
    at com.starrocks.sql.optimizer.task.SeriallyTaskScheduler.executeTasks(SeriallyTaskScheduler.java:50) ~[starrocks-fe.jar:?]
    at com.starrocks.sql.optimizer.Optimizer.memoOptimize(Optimizer.java:900) ~[starrocks-fe.jar:?]
    at com.starrocks.sql.optimizer.Optimizer.optimizeByCost(Optimizer.java:272) ~[starrocks-fe.jar:?]
    at com.starrocks.sql.optimizer.Optimizer.optimize(Optimizer.java:196) ~[starrocks-fe.jar:?]
    at com.starrocks.sql.StatementPlanner.createQueryPlanWithReTry(StatementPlanner.java:348) ~[starrocks-fe.jar:?]
    at com.starrocks.sql.StatementPlanner.plan(StatementPlanner.java:138) ~[starrocks-fe.jar:?]
    at com.starrocks.sql.StatementPlanner.plan(StatementPlanner.java:95) ~[starrocks-fe.jar:?]
    at com.starrocks.qe.StmtExecutor.execute(StmtExecutor.java:580) ~[starrocks-fe.jar:?]
    at com.starrocks.qe.ConnectProcessor.handleQuery(ConnectProcessor.java:389) ~[starrocks-fe.jar:?]
    at com.starrocks.qe.ConnectProcessor.dispatch(ConnectProcessor.java:598) ~[starrocks-fe.jar:?]
    at com.starrocks.qe.ConnectProcessor.processOnce(ConnectProcessor.java:936) ~[starrocks-fe.jar:?]
    at com.starrocks.mysql.nio.ReadListener.lambda$handleEvent$0(ReadListener.java:69) ~[starrocks-fe.jar:?]
    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128) ~[?:?]
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628) ~[?:?]
    at java.lang.Thread.run(Thread.java:829) ~[?:?]
2025-08-09 08:22:30.932-06:00 INFO (TaskCleaner|111)
[TaskManager.dropTasks():392] drop tasks:[]
2025-08-09 08:22:35.431-06:00 INFO (main|1)
[StarRocksFE.start():123] StarRocks FE starting, version: 3.3.14-ee-5b29ea9
```

The FE being promoted to leader fails:

```
    at com.starrocks.common.util.Daemon.run(Daemon.java:109) ~[starrocks-fe.jar:?]
2025-08-09 08:16:07.844-06:00 ERROR (stateChangeExecutor|86)
[GlobalStateMgr.transferToLeader():1333] failed to init journal after transfer to leader! will exit
com.starrocks.journal.JournalException: catch exception after retried 3 times
    at com.starrocks.journal.bdbje.BDBJEJournal.open(BDBJEJournal.java:222) ~[starrocks-fe.jar:?]
    at com.starrocks.server.GlobalStateMgr.transferToLeader(GlobalStateMgr.java:1323) ~[starrocks-fe.jar:?]
    at com.starrocks.server.GlobalStateMgr$1.transferToLeader(GlobalStateMgr.java:795) ~[starrocks-fe.jar:?]
    at com.starrocks.ha.StateChangeExecutor.runOneCycle(StateChangeExecutor.java:125) ~[starrocks-fe.jar:?]
    at com.starrocks.common.util.Daemon.run(Daemon.java:109) ~[starrocks-fe.jar:?]
Caused by: com.sleepycat.je.rep.InsufficientReplicasException: (JE 18.3.20) Commit policy:
SIMPLE_MAJORITY required 1 replica. But none were active with this master.
    at com.sleepycat.je.rep.impl.node.DurabilityQuorum.ensureReplicasForCommit(DurabilityQuorum.java:116) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.rep.impl.RepImpl.txnBeginHook(RepImpl.java:1171) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.rep.txn.MasterTxn.txnBeginHook(MasterTxn.java:195) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.txn.Txn.initTxn(Txn.java:384) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.txn.Txn.<init>(Txn.java:288) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.txn.Txn.<init>(Txn.java:267) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.rep.txn.MasterTxn.<init>(MasterTxn.java:146) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.rep.txn.MasterTxn$1.create(MasterTxn.java:117) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.rep.txn.MasterTxn.create(MasterTxn.java:435) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.rep.impl.RepImpl.createRepUserTxn(RepImpl.java:1145) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.txn.Txn.createAutoTxn(Txn.java:334) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.txn.LockerFactory.getWritableLocker(LockerFactory.java:79) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.Environment.setupDatabase(Environment.java:816) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.Environment.openDatabase(Environment.java:668) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.starrocks.journal.bdbje.BDBEnvironment.openDatabase(BDBEnvironment.java:446) ~[starrocks-fe.jar:?]
    at com.starrocks.journal.bdbje.BDBJEJournal.open(BDBJEJournal.java:213) ~[starrocks-fe.jar:?]
    ... 4 more
2025-08-09 08:16:07.845-06:00 INFO (Thread-69|145)
[StarRocksFE.lambda$addShutdownHook$1():374] start to execute shutdown hook
2025-08-09 08:16:07.852-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:show frontends;, QueryFEAllocatedMemory: 69224
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:show frontends;, QueryFEAllocatedMemory: 31128
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:SET NAMES utf8, QueryFEAllocatedMemory: 64464
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:SET NAMES utf8, QueryFEAllocatedMemory: 119040
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:show frontends;, QueryFEAllocatedMemory: 83760
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:show frontends;, QueryFEAllocatedMemory: 69080
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:show frontends;, QueryFEAllocatedMemory: 83760
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:show frontends;, QueryFEAllocatedMemory: 84072
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:SET NAMES utf8, QueryFEAllocatedMemory: 10048
```

**Processing Method**: Increase the FE JVM memory configuration, with a recommended minimum of 16GB in production, and adjust later based on the increment of metadata. For recovery, refer to steps 8 and 9 in the [Metadata Recovery](../administration/Meta_recovery.md) documentation.

---

### Case 8: Slow FE response leads to increased time consumption for import tasks

**Problem Phenomenon**: The agent prints `jstack` when communicating with FE and encountering a situation where FE does not respond in a timely manner. The logs of the two follower FEs contain the printed message "notify new FE type". There are a large number of `jstack` collection records in the agent log.

`jstack` collection exacerbated the unresponsive state duration of FE.

**Processing Method**: Turn off `jstack` collection. Version 3.3+ has already turned it off by default. You can remove the `jstack` command in the cluster configuration.

---

### Case 9: FE Memory Slowly Increasing (version 3.3.13 - 3.3.18)

**Version scope**: This issue occurs between versions 3.3.13 and 3.3.18.

**Problem Phenomenon**: If the FE heap memory is found to continuously and slowly increase, it may be caused by this issue.

Obtaining `jmap` and checking reveals that `com.starrocks.catalog.Replica` occupies excessive memory. All operations involving large amounts of `DROP`, `SWAP`, and `INSERT OVERWRITE` may trigger this issue.

This issue is due to the optimization of the tablet deletion path in version 3.3.13, which introduced a memory leak and was fixed in 3.3.18.

- Optimization PR in 3.3.13: [BugFix] Fix recycle bin missing to delete lake mv's expired partitions after mv refreshed
- Fix PR in [3.3.18](https://github.com/StarRocks/starrocks/pull/61582)

**Processing Method**:

- Temporary workaround: Restart FE
- Fundamental solution: Upgrade to a fixed version

**Fixed versions**: 3.3.18, 3.4.7, 3.5.4

---

## Appendix: Tool Usage Commands

### jstat — Monitor JVM GC Status

```bash
jstat -gcutil <pid> 1000 10
```

Prints the GC utilization of the FE process once every 1 second, continuously printing 10 times.

**Common indicators:**

| Indicator | Meaning                                       |
|-----------|-----------------------------------------------|
| S0, S1    | Survivor space utilization (Young Generation) |
| E         | Eden space utilization (Young Generation)     |
| O         | Old Generation utilization                    |
| YGC, YGCT | Young GC count and time                       |
| FGC, FGCT | Full GC count and time                        |

**Purpose**: Quickly determine whether GC is frequent, whether the old generation is approaching full, and whether there is a Full GC.

### jmap — View JVM Memory Object Distribution

**Method 1: View objects in the current heap (including garbage objects)**

```bash
jmap -histo <pid> | head -n 30
```

Counts the number of instances and occupied size of all object types. Use this to check what the large objects in the heap are, such as `byte[]`, `String`, `HashMap`, etc.

**Method 2: Check live objects after forcing GC**

```bash
jmap -histo:live <pid> | head -n 30
```

Triggers a Full GC, then only displays objects that still survive after the GC. Use this to troubleshoot issues such as leaked objects or cache not being released.
