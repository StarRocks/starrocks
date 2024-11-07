---
displayed_sidebar: docs
---

# SHOW DICTIONARY



Shows information about dictionary objects.

## Syntax

```SQL
SHOW DICTIONARY [ <dictionary_object_name> ]
```

## Parameters

- `dictionary_object_name`: The name of the dictionary object.

## Returns

- `DictionaryId`: The unique ID of each dictionary object.
- `DictionaryName`: The name of the dictionary object.
- `DbName`: The database to which the original object belongs.
- `dictionarySource`: The name of the original object.
- `dictionaryKeys`: The key columns of the dictionary object.
- `dictionaryValues`: The value columns of the dictionary object.
- `state`: The refresh state of the dictionary object.
  - `UNINITIALIZED`: The dictionary object has just been created and has not yet been refreshed.
  - `REFRESHING`: The dictionary object is currently being refreshed.
  - `COMMITTING`: Data has been cached in each BE node's dictionary object, and some final tasks are being performed, such as updating metadata.
  - `FINISHED`: The refresh was successful and has ended.
  - `CANCELLED`: An error occurred during the refresh process, and the refresh was canceled. You need to fix the issue based on the error information in `ErrorMessage`, and then refresh the dictionary object again.
- `lastSuccessRefreshTime`: The start time of the last successful refresh of the dictionary object.
- `lastSuccessFinishedTime`: The end time of the last successful refresh of the dictionary object.
- `nextSchedulableTime`: The next time the dictionary object data will be automatically refreshed.
- `ErrorMessage`: Error message when refreshing the dictionary object fails.
- `approximated dictionaryMemoryUsage (Bytes)`: The estimated memory usage of the dictionary object cached on each BE node.

## Examples

```Plain
MySQL > SHOW DICTIONARY\G
*************************** 1. row ***************************
                              DictionaryId: 3
                            DictionaryName: dict_obj
                                    DbName: example_db
                          dictionaryObject: dict
                            dictionaryKeys: [order_uuid]
                          dictionaryValues: [order_id_int]
                                    status: FINISHED
                    lastSuccessRefreshTime: 2024-05-23 16:12:03
                   lastSuccessFinishedTime: 2024-05-23 16:12:12
                       nextSchedulableTime: disable auto schedule for refreshing
                              ErrorMessage: 
approximated dictionaryMemoryUsage (Bytes): 172.26.82.208:8060 : 30
                                             172.26.82.210:8060 : 30
                                             172.26.82.209:8060 : 30
*************************** 2. row ***************************
                              DictionaryId: 4
                            DictionaryName: dimension_obj
                                    DbName: example_db
                          dictionaryObject: ProductDimension
                            dictionaryKeys: [ProductKey]
                          dictionaryValues: [ProductName, Category, SubCategory, Brand, Color, Size]
                                    status: FINISHED
                    lastSuccessRefreshTime: 2024-05-23 16:12:41
                   lastSuccessFinishedTime: 2024-05-23 16:12:42
                       nextSchedulableTime: disable auto schedule for refreshing
                              ErrorMessage: 
approximated dictionaryMemoryUsage (Bytes): 172.26.82.208:8060 : 270
                                             172.26.82.210:8060 : 270
                                             172.26.82.209:8060 : 270
2 rows in set (0.00 sec)
```