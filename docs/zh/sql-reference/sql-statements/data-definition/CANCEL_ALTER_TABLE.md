---
displayed_sidebar: "Chinese"
---

# CANCEL ALTER TABLE

## 功能

取消指定表的以下变更 (Alter) 操作：

- 表结构：增加列，删除列，调整列顺序和修改列类型。
- Rollup 索引: 创建 rollup 索引和删除 rollup 索引。

该语句为同步操作，只有拥有该表 `ALTER_PRIV` 权限的用户才可以使用。

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
