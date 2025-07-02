---
displayed_sidebar: docs
---

# inspect_memory

`inspect_memory(module_name)`

此函数返回指定模块的估计内存使用量。

## 参数

`module_name`: 模块的名称 (VARCHAR)。

注意:
支持以下模块名称（详细信息请参阅 `MemoryUsageTracker.java` 类）:
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

## 返回值

返回表示估计内存大小的 VARCHAR 字符串（例如，"100MB"）。

## 示例
示例1: 检查当前FE的Load模块内存使用量:
```
mysql> SELECT inspect_memory('Load');
+------------------------+
| inspect_memory('Load') |
+------------------------+
| 251.8KB                |
+------------------------+
1 row in set (0.00 sec)
```

