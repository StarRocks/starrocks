---
displayed_sidebar: docs
---

# load_tracking_logs

`load_tracking_logs` は、ロードジョブのエラーログを提供します。このビューは StarRocks v3.0 以降でサポートされています。

`load_tracking_logs` には次のフィールドが提供されています:

| **Field**     | **Description**                            |
| ------------- | ------------------------------------------ |
| ID            | ロードの ID。                              |
| JOB_ID        | ロードジョブの ID (非推奨)。               |
| LABEL         | ロードジョブのラベル。                     |
| DATABASE_NAME | ロードジョブが属するデータベース。         |
| TRACKING_LOG  | ロードジョブのエラー (あれば)。            |

:::tip
ビュー `load_tracking_logs` をクエリするには、`JOB_ID` または `LABEL` でフィルタリングする必要があります。

ロードジョブの `JOB_ID` または `LABEL` は、ビュー `information_schema.loads` から取得できます。

例:

```sql
SELECT * from information_schema.load_tracking_logs WHERE label ='user_behavior'\G
*************************** 1. row ***************************
       JOB_ID: 10141
        LABEL: user_behavior
DATABASE_NAME: mydatabase
 TRACKING_LOG: NULL
         TYPE: BROKER
1 row in set (0.02 sec)
```
:::