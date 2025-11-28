---
displayed_sidebar: docs
---

# fe_threads

`fe_threads` provides information about the threads running on each FE node.

The following fields are provided in `fe_threads`:

| **Field**      | **Description**                                              |
| -------------- | ------------------------------------------------------------ |
| FE_ADDRESS     | Address of the FE node (format: `host:port`).               |
| THREAD_ID      | Java thread ID.                                              |
| THREAD_NAME    | Name of the thread.                                          |
| THREAD_STATE   | State of the thread. Possible values: `NEW`, `RUNNABLE`, `BLOCKED`, `WAITING`, `TIMED_WAITING`, `TERMINATED`. |
| IS_DAEMON      | Indicates whether the thread is a daemon thread (`true`) or not (`false`). |
| PRIORITY       | Thread priority (typically 1-10).                           |
| CPU_TIME_MS    | CPU time consumed by the thread in milliseconds. Returns `-1` if CPU time measurement is not supported. |
| USER_TIME_MS   | User time consumed by the thread in milliseconds. Returns `-1` if user time measurement is not supported. |

## Usage Examples

### Query all threads

```sql
SELECT * FROM information_schema.fe_threads;
```

### Query threads by state

```sql
SELECT thread_id, thread_name, thread_state 
FROM information_schema.fe_threads 
WHERE thread_state = 'RUNNABLE';
```

### Count threads by state

```sql
SELECT thread_state, COUNT(*) AS count 
FROM information_schema.fe_threads 
GROUP BY thread_state 
ORDER BY count DESC;
```

### Find daemon threads

```sql
SELECT thread_id, thread_name, thread_state 
FROM information_schema.fe_threads 
WHERE is_daemon = true;
```

### Find threads with highest CPU usage

```sql
SELECT thread_id, thread_name, cpu_time_ms 
FROM information_schema.fe_threads 
WHERE cpu_time_ms >= 0 
ORDER BY cpu_time_ms DESC 
LIMIT 10;
```

### Search for specific thread names

```sql
SELECT thread_id, thread_name, thread_state 
FROM information_schema.fe_threads 
WHERE thread_name LIKE '%main%' 
   OR thread_name LIKE '%GC%';
```

### Count threads by daemon status

```sql
SELECT is_daemon, COUNT(*) AS count 
FROM information_schema.fe_threads 
GROUP BY is_daemon;
```

### Get thread statistics

```sql
SELECT 
    COUNT(*) AS total_threads,
    COUNT(DISTINCT thread_state) AS distinct_states,
    COUNT(DISTINCT thread_name) AS distinct_names,
    SUM(CASE WHEN is_daemon THEN 1 ELSE 0 END) AS daemon_count,
    SUM(CASE WHEN NOT is_daemon THEN 1 ELSE 0 END) AS non_daemon_count
FROM information_schema.fe_threads;
```
