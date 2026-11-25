---
displayed_sidebar: docs
description: "load_tracking_logs provides error logs of load jobs."
products: [oss, enterprise, cloud, anywhere]
---

# load_tracking_logs

`load_tracking_logs` provides error logs of load jobs. This view is supported from StarRocks v3.0 onwards.

The error log of a load job can contain the rows that the job rejected, so `load_tracking_logs` returns it only to users who hold the SELECT privilege on each table that the job loads data into. Users who hold another privilege on each of those tables, or the OPERATE privilege, see the job with `TRACKING_LOG` set to NULL. The `root` user can see all error logs. The error log of a load job whose target table has been dropped is visible only to the `root` user.

The following fields are provided in `load_tracking_logs`:

| **Field**     | **Description**                            |
| ------------- | ------------------------------------------ |
| ID            | The ID of the load.                        |
| JOB_ID        | The ID of the load job(deprecated).                    |
| LABEL         | The label of the load job.                 |
| DATABASE_NAME | The database that the load job belongs to. |
| TRACKING_LOG  | Errors (if any) of the load job.           |
| TYPE          | The type of the load job, such as `BROKER`, `INSERT`, `ROUTINE_LOAD`, or `STREAM_LOAD`. |

:::tip
To query the view `load_tracking_logs` you will need to filter on either a `JOB_ID` or a `LABEL`.

You can retrieve the `JOB_ID` or `LABEL` of a load job from the view `information_schema.loads`.

Example:

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
