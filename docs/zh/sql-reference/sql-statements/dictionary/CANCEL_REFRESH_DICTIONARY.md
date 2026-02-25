---
displayed_sidebar: docs
---

# CANCEL REFRESH DICTIONARY



取消刷新字典对象。

## 语法

```SQL
CANCEL REFRESH DICTIONARY <dictionary_object_name>
```

## 参数说明

`dictionary_object_name`：处于`REFRESHING`状态的字典对象名称。

## 示例

取消刷新字典对象 `dict_obj`。

```Plain
MySQL > CANCEL REFRESH DICTIONARY dict_obj;
Query OK, 0 rows affected (0.01 sec)
```