---
displayed_sidebar: docs
---

# inspect_memory

`inspect_memory(module_name)`

This function returns the estimated memory usage for a specified module.

## Arguments

`module_name`: The name of the module (VARCHAR).

NOTE:
The following module names are supported (for more details, see the `MemoryUsageTracker.java` class):
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

## Return Value

Returns a VARCHAR string representing the estimated memory size (e.g., "100MB").

## Examples
Example 1: Inspect current FE's Load module memory usage:
```
mysql> SELECT inspect_memory('Load');
+------------------------+
| inspect_memory('Load') |
+------------------------+
| 251.8KB                |
+------------------------+
1 row in set (0.00 sec)
```