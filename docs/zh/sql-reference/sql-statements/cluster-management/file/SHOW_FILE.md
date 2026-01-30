---
displayed_sidebar: docs
---

# SHOW FILE

SHOW FILE 语句用于查看保存在数据库中的文件的信息。

:::tip

该操作需要用户有 File 所属数据库上的任意权限。请参考 [GRANT](../../account-management/GRANT.md) 为用户赋权。当一个文件归属于一个数据库时，对该数据库拥有访问权限的用户都可以使用该文件。

:::

## 语法

```SQL
SHOW FILE [FROM database]
```

语句返回的信息如下：

- `Id`: 文件的全局唯一 ID。

- `DbName`: 文件所属的数据库。

- `GlobalStateMgr`: 文件所属的分类（对应创建文件时指定的 `catalog` 属性）。

- `FileName`: 文件名。

- `FileSize`: 文件大小，单位字节。

- `IsContent`: 表示文件内容是否已完全上传并存储 (true 或 false)。

- `MD5`: 消息摘要算法，用于检查文件。

## 示例

查看保存在数据库 `test_db` 中的文件。

```Plain
mysql> SHOW FILE FROM test_db;

+-------+---------+----------------+---------------+----------+-----------+----------------------------------+
| Id    | DbName  | GlobalStateMgr | FileName      | FileSize | IsContent | MD5                              |
+-------+---------+----------------+---------------+----------+-----------+----------------------------------+
| 24016 | test_db | kafka          | my_secret.txt | 43       | true      | a7c71293869ec817515e61a9f0cfb48e |
| 24072 | test_db | configs        | db_config.txt | 18       | true      | b1a32aaac8a739eac7b55a4364df8876 |
+-------+---------+----------------+---------------+----------+-----------+----------------------------------+
2 rows in set (0.01 sec)
```
