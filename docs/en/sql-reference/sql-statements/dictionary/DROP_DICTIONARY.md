---
displayed_sidebar: docs
---

# DROP DICTIONARY



Delete a dictionary object or clear the cached data within a dictionary object.

## Syntax

```SQL
DROP DICTIONARY <dictionary_object_name> [ CACHE ]
```

## Parameters

- `dictionary_object_name`: The name of the dictionary object.
- `CACHE`: If the keyword `CACHE` is specified, only the cached data within the dictionary object will be cleared. To restore the cached data later, you can manually refresh it. If the keyword `CACHE` is not specified, the dictionary object will be deleted.

## Examples

- Example 1: Clear only the cached data within the dictionary object.

```Plain
DROP DICTIONARY dict_obj CACHE;
```

  The dictionary object still exists.

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

- Example 2: Delete the dictionary object `dict_obj`.

```Plain
DROP DICTIONARY dict_obj;
```

  The dictionary object is completely deleted and no longer exists.

```Plain
MySQL > SHOW DICTIONARY dict_obj;
Empty set (0.00 sec)
```