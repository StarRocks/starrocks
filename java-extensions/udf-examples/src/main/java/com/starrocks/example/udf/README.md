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

add two parameter
```
CREATE AGGREGATE FUNCTION json_agg(String, String)
RETURNS string
properties (
	"symbol" = "com.starrocks.example.udf.JsonAgg",
	"type" = "StarrocksJar",
	"file" = "http://$HTTP_HOST:$PORT/xxx.jar"
);
```
#### usage:
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
