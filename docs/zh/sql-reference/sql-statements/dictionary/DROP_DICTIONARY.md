---
displayed_sidebar: docs
---

# DROP DICTIONARY



删除字典对象或者清空字典对象中缓存的数据。

## 语法

```SQL
DROP DICTIONARY <dictionary_object_name> [ CACHE ]
```

## 参数说明

- `dictionary_object_name`：字典对象的名称。
- `CACHE`：如果写关键词 `CACHE`，则表示仅清空字典对象中缓存的数据，后续需要恢复字典对象中的缓存数据，则可以手动刷新。如果不写关键词 `CACHE`，则表示删除字典对象。

## 示例

- **示例一**：只清空字典对象中缓存的数据。

    ```Plain
    DROP DICTIONARY dict_obj CACHE;
    ```

    该字典对象依旧存在。

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

- **示例二**：删除字典对象 `dict_obj`。

    ```Plain
    DROP DICTIONARY dict_obj;
    ```

    该字典对象完全删除，不再存在。

    ```Plain
    MySQL > SHOW DICTIONARY dict_obj;
    Empty set (0.00 sec)
    ```