---
displayed_sidebar: docs
---

# REFRESH DICTIONARY



手动刷新字典对象，内部实现时系统会查询原始对象的最新数据并写入字典对象中。

## 语法

```SQL
REFRESH DICTIONARY <dictionary_object_name>
```

## 参数说明

`dictionary_object_name`：字典对象的名称。

## 示例

手动刷新字典对象 `dict_obj`。

```Plain
MySQL > REFRESH DICTIONARY dict_obj;
Query OK, 0 rows affected (0.01 sec)
```