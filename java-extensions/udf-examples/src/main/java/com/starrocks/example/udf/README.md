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
