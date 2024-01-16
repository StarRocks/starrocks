---
displayed_sidebar: "Chinese"
---

# SHOW FILE

SHOW FILE 语句用于查看保存在数据库中的文件的信息。

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
