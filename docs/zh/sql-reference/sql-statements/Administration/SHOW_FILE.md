---
displayed_sidebar: "Chinese"
---

# SHOW FILE

SHOW FILE 语句用于查看保存在数据库中的文件的信息。

:::tip

该操作需要用户有 File 所属数据库上的任意权限。请参考 [GRANT](../account-management/GRANT.md) 为用户赋权。当一个文件归属于一个数据库时，对该数据库拥有访问权限的用户都可以使用该文件。

:::

## 语法

```SQL
SHOW FILE [FROM database]
```

语句返回的信息如下：

- `FileId`: 文件ID，全局唯一。
- `DbName`: 所属数据库。
- `Catalog`: 自定义类别。
- `FileName`: 文件名。
- `FileSize`: 文件大小，单位字节。
- `MD5`: 消息摘要算法，用于检查文件。

## 示例

查看保存在数据库 `my_database` 中的文件。

```SQL
SHOW FILE FROM my_database;
```
