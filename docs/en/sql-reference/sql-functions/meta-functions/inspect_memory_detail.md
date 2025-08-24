---
displayed_sidebar: docs
---

# inspect_memory_detail

`inspect_memory_detail(module_name, class_info)`

This function returns the estimated memory usage for a specific class or field within a module.

## Arguments

`module_name`: The name of the module (VARCHAR).
`class_info`: The name of the class or 'class_name.field_name' (VARCHAR).

NOTE:
1. The following module names are supported (for more details, see the `MemoryUsageTracker.java` class):
- Load
- Compaction
- Export
- Delete
- Transaction
- Backup
- Task
- TabletInvertedIndex
- LocalMetastore
- Report
- MV
- Query
- Profile
- Agent
- Statistics
- Coordinator
- Dict
2. `class_info`  can be a class or a class's field of the specific class(for more details, see the `MemoryUsageTracker.java` class).
  
## Return Value

Returns a VARCHAR string representing the estimated memory size (e.g., "100MB").

## Examples

Example 1: Inspect Task module TaskManager's memory usage:
```
mysql> select inspect_memory_detail('Task', 'TaskManager');
+----------------------------------------------+
| inspect_memory_detail('Task', 'TaskManager') |
+----------------------------------------------+
| 0B                                           |
+----------------------------------------------+
1 row in set (0.00 sec)

```

Example 2: Inspect Task module TaskManager's taskRunManager field's memory usage:
```
mysql> select inspect_memory_detail('Task', 'TaskManager.taskRunManager');
+-------------------------------------------------------------+
| inspect_memory_detail('Task', 'TaskManager.taskRunManager') |
+-------------------------------------------------------------+
| 592B                                                        |
+-------------------------------------------------------------+
1 row in set (0.01 sec)


```