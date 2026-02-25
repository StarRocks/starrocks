---
displayed_sidebar: docs
---

# CANCEL REFRESH DICTIONARY

辞書オブジェクトのリフレッシュをキャンセルします。

## 構文

```SQL
CANCEL REFRESH DICTIONARY <dictionary_object_name>
```

## パラメータ

- **dictionary_object_name**: REFRESHING 状態にある辞書オブジェクトの名前。

## 例

辞書オブジェクト `dict_obj` のリフレッシュをキャンセルします。

```Plain
MySQL > CANCEL REFRESH DICTIONARY dict_obj;
Query OK, 0 rows affected (0.01 sec)
```