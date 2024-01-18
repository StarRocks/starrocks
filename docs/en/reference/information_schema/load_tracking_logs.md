---
displayed_sidebar: "English"
---

# load_tracking_logs

`load_tracking_logs` provides error logs of load jobs. This view is supported from StarRocks v3.0 onwards.

The following fields are provided in `load_tracking_logs`:

| **Field**     | **Description**                            |
| ------------- | ------------------------------------------ |
| JOB_ID        | The ID of the load job.                    |
| LABEL         | The label of the load job.                 |
| DATABASE_NAME | The database that the load job belongs to. |
| TRACKING_LOG  | Errors (if any) of the load job.           |

:::tip
To query the `load_tracking_logs` you will need to filter on either a `JOB_ID` or a `LABEL`. You can retrieve labels from `information_schema.loads`.

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
