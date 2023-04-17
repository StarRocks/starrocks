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

<<<<<<< HEAD
Supplement the default values when inserting.
=======
Supplement the default values when inserting data.
>>>>>>> 9a524ee1d ([Doc] fix lingual errors in hll functions (#21723))

```plain text
insert into hllDemo(k1,v1) values(10,hll_empty());
```

<<<<<<< HEAD
Supplement the default values when importing data.
=======
Supplement the default values when loading data.
>>>>>>> 9a524ee1d ([Doc] fix lingual errors in hll functions (#21723))

```plain text
curl --location-trusted -u root: \
    -H "columns: temp1, temp2, col1=hll_hash(temp1), col2=hll_empty()" \
    -T example7.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table7/_stream_load
```
