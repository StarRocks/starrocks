---
keywords: ['xiugai'] 
displayed_sidebar: docs
---

# CANCEL ALTER TABLE

## 功能

<<<<<<< HEAD
取消指定表的以下变更 (Alter) 操作：

- 表结构：增加列，删除列，调整列顺序和修改列类型。
- Rollup 索引: 创建 rollup 索引和删除 rollup 索引。

> **注意**
>
> 该语句为同步操作，只有拥有对应表 ALTER 权限的用户才可以执行该操作。

## 语法

- 取消表结构变更操作。

    ```SQL
    CANCEL ALTER TABLE COLUMN FROM [db_name.]table_name
    ```

- 取消 rollup 索引变更操作。

    ```SQL
    CANCEL ALTER TABLE ROLLUP FROM [db_name.]table_name
    ```

## 参数说明

| **参数**   | **必选** | **说明**                                         |
| ---------- | -------- | ------------------------------------------------ |
| db_name    | 否       | 表所在的数据库名称。如不指定，默认为当前数据库。 |
| table_name | 是       | 表名。                                           |

## 示例

示例一：取消数据库 `example_db` 中，`example_table` 的表结构变更操作。

```SQL
CANCEL ALTER TABLE COLUMN FROM example_db.example_table;
```

示例二：取消当前数据库中，`example_table` 的 rollup 索引变更操作。

```SQL
CANCEL ALTER TABLE ROLLUP FROM example_table;
```
=======
取消正在进行的 ALTER TABLE 操作，包括：

- 修改列。
- 优化表结构（自 3.2 版本起），包括修改分桶方式和分桶数量。
- 创建和删除 rollup index。

> **注意**
>
> - 该语句为同步操作。
> - 只有拥有对应表 ALTER 权限的用户才可以执行语句。
> - 该语句仅支持取消使用 ALTER TABLE 执行的异步操作（如上），不支持取消使用 ALTER TABLE 执行同步操作，比如 rename。

## 语法

   ```SQL
   CANCEL ALTER TABLE { COLUMN | OPTIMIZE | ROLLUP } FROM [<db_name>.]<table_name>
   ```

## 参数说明

- `{COLUMN ｜ OPTIMIZE | ROLLUP}`：从 `COLUMN`、`OPTIMIZE` 和 `ROLLUP` 中必选其中一个。
  - 如果指定了 `COLUMN`，该语句用于取消修改列的操作。
  - 如果指定了 `OPTIMIZE`，该语句用于取消优化表结构的操作（修改分桶方式和分桶数量）。
  - 如果指定了 `ROLLUP`，该语句用于取消创建或删除 rollup index 的操作。
- `db_name`：可选。表所在的数据库名称。如不指定，默认为当前数据库。
- `table_name`：必选。表名。

## 示例

1. 取消数据库 `example_db` 中表 `example_table` 的修改列操作。

   ```SQL
   CANCEL ALTER TABLE COLUMN FROM example_db.example_table;
   ```

2. 取消数据库 `example_db` 中表 `example_table` 的优化表结构操作。

   ```SQL
   CANCEL ALTER TABLE OPTIMIZE FROM example_db.example_table;
   ```

3. 取消当前数据库中，`example_table` 的创建和删除 rollup index 的操作。

    ```SQL
    CANCEL ALTER TABLE ROLLUP FROM example_table;
    ```
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
