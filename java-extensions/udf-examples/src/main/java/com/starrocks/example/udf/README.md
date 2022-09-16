# UDF ADD

add two parameter
```
CREATE FUNCTION udf_add(int, int)
RETURNS int
properties (
	"symbol" = "com.starrocks.example.udf.UDFAdd",
	"type" = "StarrocksJar",
	"file" = "http://$HTTP_HOST:$PORT/xxx.jar"
);
```

usage:
```
> select udf_add(1, 2);
3
> select udf_add(NULL, 1);
NULL
```



# UDF JSON_AGG
#### 功能：
用于返回选中字段一作为key，字段二作为value的聚合后的json。
#### 语法：
```
json_agg(columnA, columnB)
```
#### 参数说明：
 * columnA: 作为key的列
 * columnB: 作为value的列
#### 返回值说明
   返回columnA列的值作为key，columnB的列作为value，且只保留key不为null，相同key的value值会被覆盖的json字符串。
#### 函数创建：
```
CREATE AGGREGATE FUNCTION json_agg(String, String)
RETURNS string
properties (
	"symbol" = "com.starrocks.example.udf.JsonAgg",
	"type" = "StarrocksJar",
	"file" = "http://$HTTP_HOST:$PORT/xxx.jar"
);
```
#### 示例：
```
mysql>select db_udf.json_agg(columnA, columnB)
>     from (
>         select "c1" as columnA, "111" as columnB
>         union all
>         select "c2" as columnA, "222" as columnB
>         union all
>         select "c1" as columnA, "333" as columnB
>         union all
>         select null as columnA, "333" as columnB
>         union all
>         select "c4" as columnA, null as columnB) tmp;

{"c1":"333","c2":"222","c4":null}
```
