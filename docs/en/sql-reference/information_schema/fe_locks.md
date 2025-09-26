---
displayed_sidebar: docs
---

# fe_locks

`fe_locks` provides information about metadata locks in StarRocks Frontend (FE).

The following fields are provided in `fe_locks`:

| **Field**      | **Description**                                              |
| -------------- | ------------------------------------------------------------ |
| lock_type      | Type of the lock (e.g., "DATABASE").                        |
| lock_object    | Object identifier that is locked (database name, table name, etc.). |
| lock_mode      | Lock mode. Valid values: `EXCLUSIVE`, `SHARED`.             |
| start_time     | When the lock was acquired.                                  |
| hold_time_ms   | Duration the lock has been held (in milliseconds).          |
| thread_info    | JSON string containing thread information (threadId, threadName). |
| granted        | Whether the lock is currently granted.                       |
| waiter_list    | Comma-separated list of threads waiting for this lock.      |

## Lock Modes

- **EXCLUSIVE**: Only one thread can hold this lock at a time.
- **SHARED**: Multiple threads can hold this lock simultaneously.

## Lock Types

- **DATABASE**: Database-level locks (when `lock_manager_enabled` is false).
- **TABLE**: Table-level locks (when `lock_manager_enabled` is true).

## Configuration

The behavior of `fe_locks` depends on the `lock_manager_enabled` configuration parameter:

- When `lock_manager_enabled = true`: Uses the new Lock Manager for centralized lock management with table-level granularity.
- When `lock_manager_enabled = false`: Uses traditional database-level locking.

## Examples

### Find long-running locks

```sql
SELECT lock_object, lock_mode, hold_time_ms, thread_info
FROM information_schema.fe_locks 
WHERE hold_time_ms > 10000  -- Locks held for more than 10 seconds
ORDER BY hold_time_ms DESC;
```

### Check for lock contention

```sql
SELECT lock_object, COUNT(*) as lock_count
FROM information_schema.fe_locks 
WHERE granted = true
GROUP BY lock_object
HAVING COUNT(*) > 1;
```

### Find waiting threads

```sql
SELECT lock_object, waiter_list
FROM information_schema.fe_locks 
WHERE waiter_list != '';
```

## Notes

- Querying `fe_locks` requires `OPERATE` privilege.
- The view provides real-time information but may impact performance on systems with high lock activity.
- Use appropriate WHERE clauses to filter results when dealing with large numbers of locks.