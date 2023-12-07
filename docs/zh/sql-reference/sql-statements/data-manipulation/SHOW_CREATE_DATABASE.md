---
displayed_sidebar: "Chinese"
---

# SHOW CREATE DATABASE

查看指定数据库的创建语句。

## 语法

```sql
SHOW CREATE DATABASE <db_name>
```

## 参数说明

`db_name`：数据库的名称，必填。

## 返回结果说明

返回两个字段：

- Database：数据库名称

- Create Database：数据库的创建语句

## 示例

```sql
mysql > show create database zj_test;
+----------+---------------------------+
| Database | Create Database           |
+----------+---------------------------+
| zj_test  | CREATE DATABASE `zj_test` |
+----------+---------------------------+
```

## 参考文档

- [CREATE DATABASE](../data-definition/CREATE_DATABASE.md)
- [SHOW DATABASES](SHOW_DATABASES.md)
- [USE](../data-definition/USE.md)
- [DESC](../Utility/DESCRIBE.md)
- [DROP DATABASE](../data-definition/DROP_DATABASE.md)
