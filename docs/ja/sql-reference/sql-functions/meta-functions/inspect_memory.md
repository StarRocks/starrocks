---
displayed_sidebar: docs
---

# inspect_memory

`inspect_memory(module_name)`

この関数は、指定されたモジュールの推定メモリ使用量を返します。

## 引数

`module_name`: モジュールの名前 (VARCHAR)。

注意:
以下のモジュール名がサポートされています（詳細については、`MemoryUsageTracker.java` クラスを参照してください）:
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

## 戻り値

推定メモリサイズを表す VARCHAR 文字列を返します（例: 「100MB」）。

## 例
例1: 現在のFEのLoadモジュールのメモリ使用量を検査する:
```
mysql> SELECT inspect_memory('Load');
+------------------------+
| inspect_memory('Load') |
+------------------------+
| 251.8KB                |
+------------------------+
1 row in set (0.00 sec)
```

