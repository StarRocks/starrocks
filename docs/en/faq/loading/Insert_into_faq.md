---
displayed_sidebar: "English"
---

# Insert Into

## When performing data insert, each insert in SQL takes up 50 to 100ms. Is there any way to increase efficiency?

It is not recommended to insert data piece by piece to OLAP. It is usually inserted in batches. Both methods take up the same amount of time.

## 'Insert into select' task reports error: index channel has intoleralbe failure

You can solve this problem by changing the timeout duration for the Stream Load RPC. Change the following item in **be.conf** and restart the machines to allow the change to take effect:

`streaming_load_rpc_max_alive_time_sec`: The RPC timeout for Stream Load. Unit: Seconds. Default: `1200`.

Or you can set the query timeout using the following variable:

`query_timeout`: The timeout duration for queries. Its unit is seconds, and the default value is `300`.

## The error "execute timeout" occurs when I run the INSER INTO SELECT command to load a large volume of data

By default, the query timeout duration is 300s. You can set the variable `query_timeout` to extend this duration. The unit is second.
