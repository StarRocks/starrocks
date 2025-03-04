---
displayed_sidebar: docs
---

# SHOW DICTIONARY

辞書オブジェクトに関する情報を表示します。

## 構文

```SQL
SHOW DICTIONARY [ <dictionary_object_name> ]
```

## パラメータ

- `dictionary_object_name`: 辞書オブジェクトの名前。

## 戻り値

- `DictionaryId`: 各辞書オブジェクトのユニークID。
- `DictionaryName`: 辞書オブジェクトの名前。
- `DbName`: 元のオブジェクトが属するデータベース。
- `dictionarySource`: 元のオブジェクトの名前。
- `dictionaryKeys`: 辞書オブジェクトのキー列。
- `dictionaryValues`: 辞書オブジェクトの値列。
- `state`: 辞書オブジェクトのリフレッシュ状態。
  - `UNINITIALIZED`: 辞書オブジェクトが作成されたばかりで、まだリフレッシュされていない状態。
  - `REFRESHING`: 辞書オブジェクトが現在リフレッシュ中の状態。
  - `COMMITTING`: 各 BE ノードの辞書オブジェクトにデータがキャッシュされ、メタデータの更新などの最終タスクが実行されている状態。
  - `FINISHED`: リフレッシュが成功し、終了した状態。
  - `CANCELLED`: リフレッシュプロセス中にエラーが発生し、リフレッシュがキャンセルされた状態。`ErrorMessage` のエラー情報に基づいて問題を修正し、再度辞書オブジェクトをリフレッシュする必要があります。
- `lastSuccessRefreshTime`: 辞書オブジェクトの最後の成功したリフレッシュの開始時間。
- `lastSuccessFinishedTime`: 辞書オブジェクトの最後の成功したリフレッシュの終了時間。
- `nextSchedulableTime`: 辞書オブジェクトデータが自動的にリフレッシュされる次の時間。
- `ErrorMessage`: 辞書オブジェクトのリフレッシュが失敗した際のエラーメッセージ。
- `approximated dictionaryMemoryUsage (Bytes)`: 各 BE ノードにキャッシュされた辞書オブジェクトの推定メモリ使用量。

## 例

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