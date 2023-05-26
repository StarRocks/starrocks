# hll_empty

## Description

Generates an empty HLL column to supplement the default values when inserting or loading data.

## Syntax

```Haskell
HLL_EMPTY()
```

## Return value

Returns an empty HLL.

## Examples

Supplement the default values when inserting data.

```plain text
insert into hllDemo(k1,v1) values(10,hll_empty());
```

Supplement the default values when loading data.

```plain text
curl --location-trusted -u <username>:<password> \
    -H "columns: temp1, temp2, col1=hll_hash(temp1), col2=hll_empty()" \
    -T example7.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table7/_stream_load
```
