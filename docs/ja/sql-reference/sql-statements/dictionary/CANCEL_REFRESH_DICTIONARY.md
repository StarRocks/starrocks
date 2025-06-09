---
displayed_sidebar: docs
---

# CANCEL REFRESH DICTIONARY

辞書オブジェクトのリフレッシュをキャンセルします。

## Syntax

```SQL
CANCEL REFRESH DICTIONARY <dictionary_object_name>
```

## Parameters

- **dictionary_object_name**: REFRESHING 状態にある辞書オブジェクトの名前。

## Examples

辞書オブジェクト `dict_obj` のリフレッシュをキャンセルします。

```Plain
MySQL > CANCEL REFRESH DICTIONARY dict_obj;
Query OK, 0 rows affected (0.01 sec)
```