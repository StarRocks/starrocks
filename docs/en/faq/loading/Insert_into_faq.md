# Insert Into

## When performing data insert, each insert in SQL takes up 50 to 100ms. Is there any way to increase efficiency?

It is not recommended to insert data piece by piece to OLAP. It is usually inserted in batches. Both methods take up the same amount of time.

## 'Insert into select' task reports error: index channel has intoleralbe failure

This can be solved by changing the timeout limit for stream load RPC. Also, change the following two items in fe.conf and be.conf to larger values (you can also make the adjustment in the Manager page):

```plain text
streaming_load_rpc_max_alive_time_sec=2400
tablet_writer_open_rpc_timeout_sec=120
```

## The error "execute timeout" occurs when I run the INSER INTO SELECT command to load a large volume of data

By default, the query timeout duration is 300s. You can set the `query_timeout` parameter to extend this duration. The unit is second.
