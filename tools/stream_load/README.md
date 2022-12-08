stream load import tools ( multi-threading parallel ) , expect memory use in 500M no matter file size

### Use

First , you need to install jdk , and run the script

```shell
./stream-load-import.sh --url=http://{fe_ip}:{fe_http_port}/api/{database_name}/{table_name}/_stream_load \
--source_file=/file/path/name.csv \
--H=column_separator:, \
--u=sr_username:sr_password
```

### Params

Necessary:

- --source_file: the absolute path of import file
- --url: the `fe` url , it should contain protocol and so on, not `be` redirect url
- --u: the starrocks database user and password ,not server

Optional:

- --enable_debug: default false ,`--enable_debug=true`
- --max_threads: parallel thread number , default min(server_core_number,32) ,`--max_threads=16`
- --timeout: http protocol connect timeout and proccess timeout , default 60*1000ms, for example 5s `--timeout=5000`
- --queue_size: memory queue limit size , default 256 , not reset unless you have a lot of memory and you need reset `-Xmx`
- --H: http request header , for example `--H=column_separator:,`,column_separator as key,`,`as value

Other:
https://docs.starrocks.io/en-us/latest/loading/StreamLoad

### Warn
When appear error a certain thread, other normal thread transaction will not rollback

Currently, you need ensure your file not contain error data,and can clear table

We are realizing union transaction , it can resolve this problem
