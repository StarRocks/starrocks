---
displayed_sidebar: docs
---

# fe_locks

`fe_locks` 提供 StarRocks FE 中元数据锁的信息。

`fe_locks` 提供以下字段：

| **字段**       | **描述**                                                     |
| -------------- | ------------------------------------------------------------ |
| lock_type      | 锁的类型（例如："DATABASE"）。                               |
| lock_object    | 被锁定的对象标识符（数据库名、表名等）。                     |
| lock_mode      | 锁模式。有效值：`EXCLUSIVE`、`SHARED`。                     |
| start_time     | 锁被获取的时间。                                             |
| hold_time_ms   | 锁被持有的持续时间（毫秒）。                                 |
| thread_info    | 包含线程信息的 JSON 字符串（threadId, threadName）。        |
| granted        | 锁是否当前被授予。                                           |
| waiter_list    | 等待此锁的线程列表（逗号分隔）。                             |

## 锁模式

- **EXCLUSIVE（排他锁）**：同一时间只有一个线程可以持有此锁。
- **SHARED（共享锁）**：多个线程可以同时持有此锁。

## 锁类型

- **DATABASE**：数据库级别锁（当 `lock_manager_enabled` 为 false 时）。
- **TABLE**：表级别锁（当 `lock_manager_enabled` 为 true 时）。

## 配置

`fe_locks` 的行为取决于 `lock_manager_enabled` 配置参数：

- 当 `lock_manager_enabled = true` 时：使用新的锁管理器进行集中式锁管理，具有表级别粒度。
- 当 `lock_manager_enabled = false` 时：使用传统的数据库级别锁定。

## 示例

### 查找长时间运行的锁

```sql
SELECT lock_object, lock_mode, hold_time_ms, thread_info
FROM information_schema.fe_locks 
WHERE hold_time_ms > 10000  -- 持有超过10秒的锁
ORDER BY hold_time_ms DESC;
```

### 检查锁竞争

```sql
SELECT lock_object, COUNT(*) as lock_count
FROM information_schema.fe_locks 
WHERE granted = true
GROUP BY lock_object
HAVING COUNT(*) > 1;
```

### 查找等待线程

```sql
SELECT lock_object, waiter_list
FROM information_schema.fe_locks 
WHERE waiter_list != '';
```

## 注意事项

- 查询 `fe_locks` 需要 `OPERATE` 权限。
- 该视图提供实时信息，但在高锁活动系统上可能影响性能。
- 在处理大量锁时，使用适当的 WHERE 子句来过滤结果。
