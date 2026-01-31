---
displayed_sidebar: docs
---

# Insert Into

## When performing data insert, each insert in SQL takes up 50 to 100ms. Is there any way to increase efficiency?

It is not recommended to insert data piece by piece to OLAP. It is usually inserted in batches. Both methods take up the same amount of time.

## 'Insert into select' task reports error: index channel has intolerable failure

You can solve this problem by changing the timeout duration for the Stream Load RPC. Change the following item in **be.conf** and restart the machines to allow the change to take effect:

`streaming_load_rpc_max_alive_time_sec`: The RPC timeout for Stream Load. Unit: Seconds. Default: `1200`.

Or you can set the INSERT timeout using the following variable:

`insert_timeout`: The timeout duration for INSERT statements. Its unit is seconds, and the default value is `14400`.

## The error "execute timeout" occurs when I run the INSERT INTO SELECT command to load a large volume of data

By default, the INSERT timeout duration is 14400s. You can set the variable `insert_timeout` to extend this duration. The unit is second.

## Why does INSERT INTO SELECT return “Reach limit of connections”?

It is because the user connection limit is reached. Increase the value of the user property `max_user_connections`.
