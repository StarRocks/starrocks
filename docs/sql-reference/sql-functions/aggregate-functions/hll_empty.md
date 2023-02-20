# hll_empty

## Description

Generates an empty HLL column to supplement the default values when inserting or importing data.

## Syntax

```Haskell
HLL_EMPTY()
```

## Return value

an empty HLL.

## Examples
supplement the default values when inserting.
```plain text
insert into hllDemo(k1,v1) values(10,hll_empty());
```
supplement the default values when importing data.
```plain text
curl --location-trusted -u root: \
    -H "columns: temp1, temp2, col1=hll_hash(temp1), col2=hll_empty()" \
    -T example7.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table7/_stream_load
```