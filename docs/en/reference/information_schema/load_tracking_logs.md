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
