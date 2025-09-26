# StarRocks sys.fe_locks System View Documentation

## English Documentation

### Overview

The `sys.fe_locks` system view provides detailed information about metadata locks in StarRocks Frontend (FE). This view is essential for monitoring lock contention, diagnosing performance issues, and understanding the locking behavior of your StarRocks cluster.

### Purpose

The `sys.fe_locks` view helps database administrators and developers:
- Monitor active metadata locks in the system
- Identify lock contention and potential deadlocks
- Analyze lock holding times and patterns
- Troubleshoot performance issues related to locking
- Understand the granularity of locks (database-level vs table-level)

### Schema

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `lock_type` | VARCHAR(64) | Type of the lock (e.g., "DATABASE") |
| `lock_object` | VARCHAR(64) | Object identifier that is locked (database name, table name, etc.) |
| `lock_mode` | VARCHAR(64) | Lock mode (e.g., "EXCLUSIVE", "SHARED") |
| `start_time` | DATETIME | When the lock was acquired |
| `hold_time_ms` | BIGINT | Duration the lock has been held (in milliseconds) |
| `thread_info` | VARCHAR(64) | JSON string containing thread information (threadId, threadName) |
| `granted` | BOOLEAN | Whether the lock is currently granted |
| `waiter_list` | VARCHAR(65535) | Comma-separated list of threads waiting for this lock |

### Lock Modes

- **EXCLUSIVE**: Only one thread can hold this lock at a time
- **SHARED**: Multiple threads can hold this lock simultaneously

### Lock Types

- **DATABASE**: Database-level locks (when `lock_manager_enabled` is false)
- **TABLE**: Table-level locks (when `lock_manager_enabled` is true)

### Configuration

The behavior of `sys.fe_locks` depends on the `lock_manager_enabled` configuration parameter:

- **When `lock_manager_enabled = true`**: Uses the new Lock Manager for centralized lock management with table-level granularity
- **When `lock_manager_enabled = false`**: Uses traditional database-level locking

### Usage Examples

#### Basic Query
```sql
SELECT * FROM sys.fe_locks;
```

#### Find Long-Running Locks
```sql
SELECT lock_object, lock_mode, hold_time_ms, thread_info
FROM sys.fe_locks 
WHERE hold_time_ms > 10000  -- Locks held for more than 10 seconds
ORDER BY hold_time_ms DESC;
```

#### Check for Lock Contention
```sql
SELECT lock_object, COUNT(*) as lock_count
FROM sys.fe_locks 
WHERE granted = true
GROUP BY lock_object
HAVING COUNT(*) > 1;
```

#### Find Waiting Threads
```sql
SELECT lock_object, waiter_list
FROM sys.fe_locks 
WHERE waiter_list != '';
```

### Performance Considerations

- Querying `sys.fe_locks` requires `OPERATE` privilege
- The view provides real-time information but may impact performance on systems with high lock activity
- Use appropriate WHERE clauses to filter results when dealing with large numbers of locks

### Troubleshooting

Common scenarios where `sys.fe_locks` is useful:

1. **Deadlock Detection**: Look for circular wait patterns in the waiter_list
2. **Performance Issues**: Identify locks held for extended periods
3. **Concurrency Problems**: Check for excessive lock contention on specific objects
4. **Import/Export Issues**: Monitor locks during data loading operations

---

## 中文文档

### 概述

`sys.fe_locks` 系统视图提供了 StarRocks 前端（FE）中元数据锁的详细信息。该视图对于监控锁竞争、诊断性能问题以及理解 StarRocks 集群的锁定行为至关重要。

### 用途

`sys.fe_locks` 视图帮助数据库管理员和开发人员：
- 监控系统中的活动元数据锁
- 识别锁竞争和潜在的死锁
- 分析锁持有时间和模式
- 排查与锁定相关的性能问题
- 理解锁的粒度（数据库级别 vs 表级别）

### 表结构

| 列名 | 数据类型 | 描述 |
|------|----------|------|
| `lock_type` | VARCHAR(64) | 锁的类型（例如："DATABASE"） |
| `lock_object` | VARCHAR(64) | 被锁定的对象标识符（数据库名、表名等） |
| `lock_mode` | VARCHAR(64) | 锁模式（例如："EXCLUSIVE"、"SHARED"） |
| `start_time` | DATETIME | 锁被获取的时间 |
| `hold_time_ms` | BIGINT | 锁被持有的持续时间（毫秒） |
| `thread_info` | VARCHAR(64) | 包含线程信息的 JSON 字符串（threadId, threadName） |
| `granted` | BOOLEAN | 锁是否当前被授予 |
| `waiter_list` | VARCHAR(65535) | 等待此锁的线程列表（逗号分隔） |

### 锁模式

- **EXCLUSIVE（排他锁）**：同一时间只有一个线程可以持有此锁
- **SHARED（共享锁）**：多个线程可以同时持有此锁

### 锁类型

- **DATABASE**：数据库级别锁（当 `lock_manager_enabled` 为 false 时）
- **TABLE**：表级别锁（当 `lock_manager_enabled` 为 true 时）

### 配置

`sys.fe_locks` 的行为取决于 `lock_manager_enabled` 配置参数：

- **当 `lock_manager_enabled = true` 时**：使用新的锁管理器进行集中式锁管理，具有表级别粒度
- **当 `lock_manager_enabled = false` 时**：使用传统的数据库级别锁定

### 使用示例

#### 基本查询
```sql
SELECT * FROM sys.fe_locks;
```

#### 查找长时间运行的锁
```sql
SELECT lock_object, lock_mode, hold_time_ms, thread_info
FROM sys.fe_locks 
WHERE hold_time_ms > 10000  -- 持有超过10秒的锁
ORDER BY hold_time_ms DESC;
```

#### 检查锁竞争
```sql
SELECT lock_object, COUNT(*) as lock_count
FROM sys.fe_locks 
WHERE granted = true
GROUP BY lock_object
HAVING COUNT(*) > 1;
```

#### 查找等待线程
```sql
SELECT lock_object, waiter_list
FROM sys.fe_locks 
WHERE waiter_list != '';
```

### 性能考虑

- 查询 `sys.fe_locks` 需要 `OPERATE` 权限
- 该视图提供实时信息，但在高锁活动系统上可能影响性能
- 在处理大量锁时，使用适当的 WHERE 子句来过滤结果

### 故障排除

`sys.fe_locks` 有用的常见场景：

1. **死锁检测**：在 waiter_list 中查找循环等待模式
2. **性能问题**：识别长时间持有的锁
3. **并发问题**：检查特定对象上的过度锁竞争
4. **导入/导出问题**：在数据加载操作期间监控锁

---

## 日本語ドキュメント

### 概要

`sys.fe_locks` システムビューは、StarRocks フロントエンド（FE）のメタデータロックに関する詳細情報を提供します。このビューは、ロック競合の監視、パフォーマンス問題の診断、および StarRocks クラスターのロック動作の理解に不可欠です。

### 目的

`sys.fe_locks` ビューは、データベース管理者と開発者が以下を行うのに役立ちます：
- システム内のアクティブなメタデータロックを監視
- ロック競合と潜在的なデッドロックを特定
- ロック保持時間とパターンを分析
- ロックに関連するパフォーマンス問題のトラブルシューティング
- ロックの粒度（データベースレベル vs テーブルレベル）を理解

### スキーマ

| 列名 | データ型 | 説明 |
|------|----------|------|
| `lock_type` | VARCHAR(64) | ロックのタイプ（例：「DATABASE」） |
| `lock_object` | VARCHAR(64) | ロックされているオブジェクト識別子（データベース名、テーブル名など） |
| `lock_mode` | VARCHAR(64) | ロックモード（例：「EXCLUSIVE」、「SHARED」） |
| `start_time` | DATETIME | ロックが取得された時刻 |
| `hold_time_ms` | BIGINT | ロックが保持されている時間（ミリ秒） |
| `thread_info` | VARCHAR(64) | スレッド情報を含む JSON 文字列（threadId, threadName） |
| `granted` | BOOLEAN | ロックが現在付与されているかどうか |
| `waiter_list` | VARCHAR(65535) | このロックを待機しているスレッドのリスト（カンマ区切り） |

### ロックモード

- **EXCLUSIVE（排他ロック）**：一度に1つのスレッドのみがこのロックを保持できます
- **SHARED（共有ロック）**：複数のスレッドが同時にこのロックを保持できます

### ロックタイプ

- **DATABASE**：データベースレベルロック（`lock_manager_enabled` が false の場合）
- **TABLE**：テーブルレベルロック（`lock_manager_enabled` が true の場合）

### 設定

`sys.fe_locks` の動作は `lock_manager_enabled` 設定パラメータに依存します：

- **`lock_manager_enabled = true` の場合**：テーブルレベル粒度で集中ロック管理を行う新しいロックマネージャーを使用
- **`lock_manager_enabled = false` の場合**：従来のデータベースレベルロックを使用

### 使用例

#### 基本クエリ
```sql
SELECT * FROM sys.fe_locks;
```

#### 長時間実行ロックの検索
```sql
SELECT lock_object, lock_mode, hold_time_ms, thread_info
FROM sys.fe_locks 
WHERE hold_time_ms > 10000  -- 10秒以上保持されているロック
ORDER BY hold_time_ms DESC;
```

#### ロック競合の確認
```sql
SELECT lock_object, COUNT(*) as lock_count
FROM sys.fe_locks 
WHERE granted = true
GROUP BY lock_object
HAVING COUNT(*) > 1;
```

#### 待機スレッドの検索
```sql
SELECT lock_object, waiter_list
FROM sys.fe_locks 
WHERE waiter_list != '';
```

### パフォーマンス考慮事項

- `sys.fe_locks` のクエリには `OPERATE` 権限が必要
- ビューはリアルタイム情報を提供しますが、高いロック活動があるシステムではパフォーマンスに影響する可能性があります
- 大量のロックを扱う場合は、適切な WHERE 句を使用して結果をフィルタリングしてください

### トラブルシューティング

`sys.fe_locks` が有用な一般的なシナリオ：

1. **デッドロック検出**：waiter_list で循環待機パターンを探す
2. **パフォーマンス問題**：長時間保持されているロックを特定
3. **並行性問題**：特定のオブジェクトでの過度のロック競合を確認
4. **インポート/エクスポート問題**：データロード操作中のロックを監視

---

## まとめ / Summary / 总结

このドキュメントは、StarRocks の `sys.fe_locks` システムビューの包括的なガイドを提供し、英語、中国語、日本語の3つの言語で詳細な説明を含んでいます。このビューは、メタデータロックの監視とトラブルシューティングに不可欠なツールです。

This document provides a comprehensive guide to StarRocks' `sys.fe_locks` system view, with detailed explanations in three languages: English, Chinese, and Japanese. This view is an essential tool for monitoring and troubleshooting metadata locks.

本文档提供了 StarRocks `sys.fe_locks` 系统视图的综合指南，包含英语、中文和日语三种语言的详细说明。该视图是监控和故障排除元数据锁的重要工具。