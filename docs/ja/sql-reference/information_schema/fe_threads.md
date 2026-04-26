---
displayed_sidebar: docs
---

# fe_threads

`fe_threads` は各 FE ノードで実行されているスレッドに関する情報を提供します。

`fe_threads` には以下のフィールドが提供されています:

| **フィールド**       | **説明**                                                     |
| -------------- | ------------------------------------------------------------ |
| FE_ADDRESS     | FE ノードのアドレス（形式：`host:port`）。                   |
| THREAD_ID      | Java スレッド ID。                                            |
| THREAD_NAME    | スレッド名。                                                  |
| THREAD_STATE   | スレッドの状態。可能な値：`NEW`、`RUNNABLE`、`BLOCKED`、`WAITING`、`TIMED_WAITING`、`TERMINATED`。 |
| IS_DAEMON      | スレッドがデーモンスレッドであるかどうか（`true`）またはそうでないか（`false`）を示します。 |
| PRIORITY       | スレッドの優先度（通常は 1-10）。                            |
| CPU_TIME_MS    | スレッドが消費した CPU 時間（ミリ秒）。CPU 時間の測定がサポートされていない場合は `-1` を返します。 |
| USER_TIME_MS   | スレッドが消費したユーザー時間（ミリ秒）。ユーザー時間の測定がサポートされていない場合は `-1` を返します。 |

## 使用例

### すべてのスレッドをクエリ

```sql
SELECT * FROM information_schema.fe_threads;
```

### 状態でスレッドをクエリ

```sql
SELECT thread_id, thread_name, thread_state 
FROM information_schema.fe_threads 
WHERE thread_state = 'RUNNABLE';
```

### 状態別にスレッド数をカウント

```sql
SELECT thread_state, COUNT(*) AS count 
FROM information_schema.fe_threads 
GROUP BY thread_state 
ORDER BY count DESC;
```

### デーモンスレッドを検索

```sql
SELECT thread_id, thread_name, thread_state 
FROM information_schema.fe_threads 
WHERE is_daemon = true;
```

### CPU 使用率が最も高いスレッドを検索

```sql
SELECT thread_id, thread_name, cpu_time_ms 
FROM information_schema.fe_threads 
WHERE cpu_time_ms >= 0 
ORDER BY cpu_time_ms DESC 
LIMIT 10;
```

### 特定のスレッド名を検索

```sql
SELECT thread_id, thread_name, thread_state 
FROM information_schema.fe_threads 
WHERE thread_name LIKE '%main%' 
   OR thread_name LIKE '%GC%';
```

### デーモンスレッドの状態でカウント

```sql
SELECT is_daemon, COUNT(*) AS count 
FROM information_schema.fe_threads 
GROUP BY is_daemon;
```

### スレッド統計情報を取得

```sql
SELECT 
    COUNT(*) AS total_threads,
    COUNT(DISTINCT thread_state) AS distinct_states,
    COUNT(DISTINCT thread_name) AS distinct_names,
    SUM(CASE WHEN is_daemon THEN 1 ELSE 0 END) AS daemon_count,
    SUM(CASE WHEN NOT is_daemon THEN 1 ELSE 0 END) AS non_daemon_count
FROM information_schema.fe_threads;
```
