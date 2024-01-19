---
displayed_sidebar: "Chinese"
---

# load_tracking_logs

提供导入作业相关的错误信息。此视图自 StarRocks v3.0 起支持。

`load_tracking_logs` 提供以下字段：

| **字段**      | **描述**                         |
| ------------- | -------------------------------- |
| JOB_ID        | 导入作业的 ID。                  |
| LABEL         | 导入作业的 Label。               |
| DATABASE_NAME | 导入作业所属的数据库名称。       |
| TRACKING_LOG  | 导入作业的错误日志信息（如有）。 |

:::tip
查询 `load_tracking_logs` 视图时，您需要根据 `JOB_ID` 或 `LABEL` 进行过滤。

您可以通过 `information_schema.loads` 视图查询已有的 `JOB_ID` 和 `LABEL` 。

示例：

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
