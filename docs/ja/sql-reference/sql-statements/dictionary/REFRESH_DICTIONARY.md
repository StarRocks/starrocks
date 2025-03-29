---
displayed_sidebar: docs
---

# REFRESH DICTIONARY

辞書オブジェクトを手動でリフレッシュします。内部的には、システムが元のオブジェクトから最新のデータをクエリし、それを辞書オブジェクトに書き込みます。

## 構文

```SQL
REFRESH DICTIONARY <dictionary_object_name>
```

## パラメータ

- **dictionary_object_name**: 辞書オブジェクトの名前。

## 例

辞書オブジェクト `dict_obj` を手動でリフレッシュします。

```Plain
MySQL > REFRESH DICTIONARY dict_obj;
Query OK, 0 rows affected (0.01 sec)
```