---
displayed_sidebar: docs
---

# Jemalloc 堆内存分析

本文介绍如何为 StarRocks 启用和可视化 Jemalloc 堆内存分析。

## 适用于 StarRocks v3.1.6 及更高版本

### 启用 Jemalloc 堆内存分析

语法：

```SQL
ADMIN EXECUTE ON <be_id> 'System.print(HeapProf.getInstance().enable_prof())'
```

`be_id`: BE/CN 节点的 ID。可以通过运行 SHOW BACKENDS 或 SHOW COMPUTE NODES 获取 ID。

示例：

```SQL
mysql> admin execute on 10001 'System.print(HeapProf.getInstance().enable_prof())';
+----------------------+
| result               |
+----------------------+
| instance of HeapProf |
+----------------------+
1 row in set (0.00 sec)
```

可以使用以下语法检查 Jemalloc 堆内存分析是否已启用：

```SQL
ADMIN EXECUTE ON <be_id> 'System.print(HeapProf.getInstance().has_enable())'
```

示例：

```SQL
mysql> admin execute on 10001 'System.print(HeapProf.getInstance().has_enable())';
+--------+
| result |
+--------+
| true   |
+--------+
1 row in set (0.01 sec)
```

可以使用以下语法禁用 Jemalloc 堆内存分析：

```SQL
ADMIN EXECUTE ON <be_id> 'System.print(HeapProf.getInstance().disable_prof())'
```

示例：

```SQL
mysql> admin execute on 10001 'System.print(HeapProf.getInstance().disable_prof())';
+----------------------+
| result               |
+----------------------+
| instance of HeapProf |
+----------------------+
1 row in set (0.00 sec)
```

### 收集 Jemalloc 堆内存分析

语法：

```SQL
ADMIN EXECUTE ON <be_id> 'System.print(HeapProf.getInstance().dump_dot_snapshot())'
```

示例：

```SQL
mysql> admin execute on 10001 'System.print(HeapProf.getInstance().dump_dot_snapshot())';
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| result                                                                                                                                                                                                          |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| digraph "/home/disk5/fha/opt/env/default/be/lib/starrocks_be; 1.0 MB" {                                                                                                                                         |
| node [width=0.375,height=0.25];                                                                                                                                                                                 |
| Legend [shape=box,fontsize=24,shape=plaintext,label="/home/disk5/fha/opt/env/default/be/lib/starrocks_be\lTotal MB: 1.0\lFocusing on: 1.0\lDropped nodes with <= 0.0 abs(MB)\lDropped edges with <= 0.0 MB\l"]; |
| N1 [label="brpc\nInputMessenger\nOnNewMessages\n0.0 (0.0%)\rof 1.0 (100.0%)\r",shape=box,fontsize=8.0];                                                                                                         |
| N2 [label="brpc\nSocket\nProcessEvent\n0.0 (0.0%)\rof 1.0 (100.0%)\r",shape=box,fontsize=8.0];                                                                                                                  |
| N3 [label="bthread\nTaskGroup\ntask_runner\n0.0 (0.0%)\rof 1.0 (100.0%)\r",shape=box,fontsize=8.0];                                                                                                             |
| N4 [label="bthread_make_fcontext\n0.0 (0.0%)\rof 1.0 (100.0%)\r",shape=box,fontsize=8.0];                                                                                                                       |
| N5 [label="brpc\nInputMessenger\nCutInputMessage\n0.0 (0.0%)\rof 0.5 (50.1%)\r",shape=box,fontsize=8.0];                                                                                                        |
| N6 [label="brpc\npolicy\nParseRpcMessage\n0.5 (50.1%)\r",shape=box,fontsize=43.4];                                                                                                                              |
| N7 [label="brpc\nProcessInputMessage\n0.0 (0.0%)\rof 0.5 (49.9%)\r",shape=box,fontsize=8.0];                                                                                                                    |
| N8 [label="brpc\npolicy\nProcessRpcRequest\n0.0 (0.0%)\rof 0.5 (49.9%)\r",shape=box,fontsize=8.0];                                                                                                              |
| N9 [label="starrocks\nPInternalServiceImplBase\nexecute_command\n0.0 (0.0%)\rof 0.5 (49.9%)\r",shape=box,fontsize=8.0];                                                                                         |
| N10 [label="starrocks\nStorageEngineRef\nbind\n0.0 (0.0%)\rof 0.5 (49.9%)\r",shape=box,fontsize=8.0];                                                                                                           |
| N11 [label="starrocks\nexecute_command\n0.0 (0.0%)\rof 0.5 (49.9%)\r",shape=box,fontsize=8.0];                                                                                                                  |
| N12 [label="starrocks\nexecute_script\n0.0 (0.0%)\rof 0.5 (49.9%)\r",shape=box,fontsize=8.0];                                                                                                                   |
| N13 [label="std\nmake_unique\n0.5 (49.9%)\r",shape=box,fontsize=43.3];                                                                                                                                          |
| N2 -> N1 [label=1.0, weight=16398, style="setlinewidth(2.000000)"];                                                                                                                                             |
| N3 -> N2 [label=1.0, weight=16398, style="setlinewidth(2.000000)"];                                                                                                                                             |
| N4 -> N3 [label=1.0, weight=16398, style="setlinewidth(2.000000)"];                                                                                                                                             |
| N1 -> N5 [label=0.5, weight=10102, style="setlinewidth(2.000000)"];                                                                                                                                             |
| N5 -> N6 [label=0.5, weight=10102, style="setlinewidth(2.000000)"];                                                                                                                                             |
| N9 -> N11 [label=0.5, weight=10086, style="setlinewidth(2.000000)"];                                                                                                                                            |
| N12 -> N10 [label=0.5, weight=10086, style="setlinewidth(2.000000)"];                                                                                                                                           |
| N11 -> N12 [label=0.5, weight=10086, style="setlinewidth(2.000000)"];                                                                                                                                           |
| N1 -> N7 [label=0.5, weight=10086, style="setlinewidth(2.000000)"];                                                                                                                                             |
| N7 -> N8 [label=0.5, weight=10086, style="setlinewidth(2.000000)"];                                                                                                                                             |
| N10 -> N13 [label=0.5, weight=10086, style="setlinewidth(2.000000)"];                                                                                                                                           |
| N8 -> N9 [label=0.5, weight=10086, style="setlinewidth(2.000000)"];                                                                                                                                             |
| }                                                                                                                                                                                                               |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
29 rows in set (30.22 sec)
```

### 可视化 Jemalloc 堆内存分析

将上一步收集的分析文本复制并粘贴到 [GraphvizOnline](https://dreampuf.github.io/GraphvizOnline/)。

然后可以下载可视化的堆内存分析。

示例：

![Example - Visualized Heap Profile - New](../_assets/visualized_heap_profile-new.png)

## 适用于 StarRocks v3.1.6 之前的版本

按照以下步骤为 StarRocks v3.1.6 之前的版本启用和可视化 Jemalloc 堆内存分析：

1. 安装 jemalloc v5.2.0 并启用 Jemalloc 堆内存分析。

   ```Bash
   wget https://github.com/jemalloc/jemalloc/archive/5.2.0.tar.gz
   tar xf 5.2.0.tar.gz
   cd jemalloc-5.2.0/
   sh autogen.sh --enable-prof
   ```

2. 修改 BE 节点的 **be/bin/start_backend.sh** 文件。在 `export JEMALLOC_CONF` 行的末尾添加一个额外的选项 `prof:true`。**记得用逗号分隔选项。**

   示例：

   ```Bash
   export JEMALLOC_CONF="percpu_arena:percpu,oversize_threshold:0,muzzy_decay_ms:5000,dirty_decay_ms:5000,metadata_thp:auto,background_thread:true,prof:true"
   ```

3. 重启 BE 节点。

4. 输出内存快照。

   ```Bash
   curl http://{$be_ip}:{$be_http_port}/pprof/heap >a.heap
   ```

   **几小时后**输出另一个堆快照。

   ```Bash
   curl http://{$be_ip}:{$be_http_port}/pprof/heap >b.heap
   ```

5. 计算快照之间的变化并生成 DOT 文件。

   ```Bash
   jeprof --dot /{$path_to_be}/lib/starrocks_be --base=a.heap b.heap >a.dot
   ```

6. 根据 DOT 文件生成 PDF 文件。

   ```Bash
   dot -Tpdf a.dot -o a.pdf
   ```

示例：

![Example - Visualized Heap Profile - Old](../_assets/visualized_heap_profile-old.png)
