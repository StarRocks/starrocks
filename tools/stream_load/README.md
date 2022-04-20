stream load import tools ( multi-threading parallel ) , expect memory use in 500M no matter file size

### Use

First , you need to install jdk , and run the script

```shell
./stream-load-import.sh --url=http://{fe_ip}:{fe_http_port}/api/{database_name}/{table_name}/_stream_load \
--source-file=/file/path/name.csv \
--H=column_separator:, \
--u=sr_username:sr_password
```

### Params

Necessary:

- --source-file: the absolute path of import file
- --url: the `fe` url , it should contain protocol and so on, not `be` redirect url
- --u: the starrocks database user and password ,not server

Optional:

- --is-debug: default false ,`--is-debug=true`
- --thread-number: parallel thread number , default min(server_core_number,32) ,`--thread-number=16`
- --timeout: http protocol connect timeout and proccess timeout , default 60*1000ms, for example 5s `--timeout=5000`
- --size: memory queue limit size , default 4096 , not reset unless you have a lot of memory
- --H: http request header , for example `--H=column_separator:,`,column_separator as key,`,`as value
 
Other:
https://docs.starrocks.com/zh-cn/main/loading/StreamLoad

### Warn
When appear error a certain thread, other normal thread transaction will not rollback

Currently, you need ensure your file not contain error data,and can clear table 

We are realizing union transaction , it can resolve this problem

