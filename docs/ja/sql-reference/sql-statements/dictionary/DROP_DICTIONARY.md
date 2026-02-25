---
displayed_sidebar: docs
---

# DROP DICTIONARY

辞書オブジェクトを削除するか、辞書オブジェクト内のキャッシュデータをクリアします。

## 構文

```SQL
DROP DICTIONARY <dictionary_object_name> [ CACHE ]
```

## パラメータ

- `dictionary_object_name`: 辞書オブジェクトの名前。
- `CACHE`: キーワード `CACHE` が指定されている場合、辞書オブジェクト内のキャッシュデータのみがクリアされます。後でキャッシュデータを復元するには、手動でリフレッシュできます。キーワード `CACHE` が指定されていない場合、辞書オブジェクトは削除されます。

## 例

- 例 1: 辞書オブジェクト内のキャッシュデータのみをクリアします。

```Plain
DROP DICTIONARY dict_obj CACHE;
```

  辞書オブジェクトはまだ存在します。

```Plain
MySQL > SHOW DICTIONARY dict_obj\G
*************************** 1. row ***************************
                              DictionaryId: 5
                            DictionaryName: dict_obj
                                    DbName: example_db
                          dictionaryObject: dict
                            dictionaryKeys: [order_uuid]
                          dictionaryValues: [order_id_int]
                                    status: UNINITIALIZED
                    lastSuccessRefreshTime: 2024-05-24 12:59:10
                   lastSuccessFinishedTime: 2024-05-24 12:59:20
                       nextSchedulableTime: disable auto schedule for refreshing
                              ErrorMessage: 
approximated dictionaryMemoryUsage (Bytes): 172.26.80.55:8060 : 0
                                             172.26.80.56:8060 : 0
                                             172.26.80.57:8060 : 0
1 row in set (0.00 sec)
```

- 例 2: 辞書オブジェクト `dict_obj` を削除します。

```Plain
DROP DICTIONARY dict_obj;
```

  辞書オブジェクトは完全に削除され、もはや存在しません。

```Plain
MySQL > SHOW DICTIONARY dict_obj;
Empty set (0.00 sec)
```