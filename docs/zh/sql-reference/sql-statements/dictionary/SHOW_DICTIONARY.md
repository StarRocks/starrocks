---
displayed_sidebar: docs
---

# SHOW DICTIONARY



查询字典对象的信息。

## 语法

```SQL
SHOW DICTIONARY [ <dictionary_object_name> ]
```

## 参数说明

`dictionary_object_name`：字典对象的名称。

## 返回

- `DictionaryId`：每个字典对象的唯一 ID。
- `DictionaryName`：字典对象名称。
- `DbName`：原始对象所属的数据库。
- `dictionarySource`：原始对象的名称。
- `dictionaryKeys`：字典对象的 key 列。
- `dictionaryValues`：字典对象的 value 列。
- `state`：字典对象的刷新状态。
  - `UNINITIALIZED`：刚创建字典对象，还未被刷新；
  - `REFRESHING`：正在刷新字典对象。
  - `COMMITTING`: 数据已经缓存在各个 BE 的字典对象中，正在做一些收尾工作，例如更新元数据信息等。
  - `FINISHED`：刷新成功，结束刷新。
  - `CANCELLED`：刷新过程中出错，已取消刷新。您需要根据 `ErrorMessage` 中的报错信息进行修复，然后再次刷新字典对象。
- `lastSuccessRefreshTime`：字典对象上次成功刷新开始的时间。
- `lastSuccessFinishedTime`：字典对象上次成功刷新结束的时间。
- `nextSchedulableTime`：下一次自动刷新字典对象的时间。
- `ErrorMessage`：刷新字典对象失败时的报错信息。
- `approximated dictionaryMemoryUsage (Bytes)`：每个 BE 节点上缓存字典对象占用内存的估计值。

## 示例

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
