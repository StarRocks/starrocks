---
displayed_sidebar: docs
---

# fe_threads

`fe_threads` 提供有关每个 FE 节点上运行的线程的信息。

`fe_threads` 提供以下字段：

| **字段**       | **描述**                                                     |
| -------------- | ------------------------------------------------------------ |
| FE_ADDRESS     | FE 节点的地址（格式：`host:port`）。                         |
| THREAD_ID      | Java 线程 ID。                                                |
| THREAD_NAME    | 线程名称。                                                    |
| THREAD_STATE   | 线程状态。可能的值：`NEW`、`RUNNABLE`、`BLOCKED`、`WAITING`、`TIMED_WAITING`、`TERMINATED`。 |
| IS_DAEMON      | 指示线程是否为守护线程（`true`）或非守护线程（`false`）。    |
| PRIORITY       | 线程优先级（通常为 1-10）。                                  |
| CPU_TIME_MS    | 线程消耗的 CPU 时间（毫秒）。如果 CPU 时间测量不受支持，则返回 `-1`。 |
| USER_TIME_MS   | 线程消耗的用户时间（毫秒）。如果用户时间测量不受支持，则返回 `-1`。 |

## 使用示例

### 查询所有线程

```sql
SELECT * FROM information_schema.fe_threads;
```

### 按状态查询线程

```sql
SELECT thread_id, thread_name, thread_state 
FROM information_schema.fe_threads 
WHERE thread_state = 'RUNNABLE';
```

### 按状态统计线程数量

```sql
SELECT thread_state, COUNT(*) AS count 
FROM information_schema.fe_threads 
GROUP BY thread_state 
ORDER BY count DESC;
```

### 查找守护线程

```sql
SELECT thread_id, thread_name, thread_state 
FROM information_schema.fe_threads 
WHERE is_daemon = true;
```

### 查找 CPU 使用率最高的线程

```sql
SELECT thread_id, thread_name, cpu_time_ms 
FROM information_schema.fe_threads 
WHERE cpu_time_ms >= 0 
ORDER BY cpu_time_ms DESC 
LIMIT 10;
```

### 搜索特定线程名称

```sql
SELECT thread_id, thread_name, thread_state 
FROM information_schema.fe_threads 
WHERE thread_name LIKE '%main%' 
   OR thread_name LIKE '%GC%';
```

### 按守护线程状态统计

```sql
SELECT is_daemon, COUNT(*) AS count 
FROM information_schema.fe_threads 
GROUP BY is_daemon;
```

### 获取线程统计信息

```sql
SELECT 
    COUNT(*) AS total_threads,
    COUNT(DISTINCT thread_state) AS distinct_states,
    COUNT(DISTINCT thread_name) AS distinct_names,
    SUM(CASE WHEN is_daemon THEN 1 ELSE 0 END) AS daemon_count,
    SUM(CASE WHEN NOT is_daemon THEN 1 ELSE 0 END) AS non_daemon_count
FROM information_schema.fe_threads;
```
