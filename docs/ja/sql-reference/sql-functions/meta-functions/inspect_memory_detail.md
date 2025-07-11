---
displayed_sidebar: docs
---

# inspect_memory_detail

`inspect_memory_detail(module_name, class_info)`

この関数は、モジュール内の特定のクラスまたはフィールドの推定メモリ使用量を返します。

## 引数

`module_name`: モジュールの名前 (VARCHAR)。
`class_info`: クラスの名前、または「class_name.field_name」 (VARCHAR)。

注意:
1. 以下のモジュール名がサポートされています（詳細については、`MemoryUsageTracker.java` クラスを参照してください）:
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
2. `class_info` は特定のクラスまたはクラスのフィールドにすることができます（詳細については、`MemoryUsageTracker.java` クラスを参照してください）。
  
## 戻り値

推定メモリサイズを表す VARCHAR 文字列を返します（例: 「100MB」）。

## 例

例1: TaskモジュールのTaskManagerのメモリ使用量を検査する:
```
mysql> select inspect_memory_detail('Task', 'TaskManager');
+----------------------------------------------+
| inspect_memory_detail('Task', 'TaskManager') |
+----------------------------------------------+
| 0B                                           |
+----------------------------------------------+
1 row in set (0.00 sec)

```

例2: TaskモジュールのTaskManagerのtaskRunManagerフィールドのメモリ使用量を検査する:
```
mysql> select inspect_memory_detail('Task', 'TaskManager.taskRunManager');
+-------------------------------------------------------------+
| inspect_memory_detail('Task', 'TaskManager.taskRunManager') |
+-------------------------------------------------------------+
| 592B                                                        |
+-------------------------------------------------------------+
1 row in set (0.01 sec)

