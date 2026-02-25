---
displayed_sidebar: docs
---

# inspect_memory_detail

`inspect_memory_detail(module_name, class_info)`

此函数返回模块中特定类或字段的估计内存使用量。

## 参数

`module_name`: 模块的名称 (VARCHAR)。
`class_info`: 类名或"class_name.field_name" (VARCHAR)。

注意:
1. 支持以下模块名称（详细信息请参阅 `MemoryUsageTracker.java` 类）:
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
2. `class_info` 可以是特定类或类的字段（详细信息请参阅 `MemoryUsageTracker.java` 类）。
  
## 返回值

返回表示估计内存大小的 VARCHAR 字符串（例如，"100MB"）。

## 示例

示例1: 检查Task模块TaskManager的内存使用量:
```
mysql> select inspect_memory_detail('Task', 'TaskManager');
+----------------------------------------------+
| inspect_memory_detail('Task', 'TaskManager') |
+----------------------------------------------+
| 0B                                           |
+----------------------------------------------+
1 row in set (0.00 sec)

```

示例2: 检查Task模块TaskManager的taskRunManager字段的内存使用量:
```
mysql> select inspect_memory_detail('Task', 'TaskManager.taskRunManager');
+-------------------------------------------------------------+
| inspect_memory_detail('Task', 'TaskManager.taskRunManager') |
+-------------------------------------------------------------+
| 592B                                                        |
+-------------------------------------------------------------+
1 row in set (0.01 sec)

