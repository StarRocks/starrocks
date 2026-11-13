---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Generates an empty HLL column to supplement the default values when inserting or loading data."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# hll_empty



Generates an empty HLL column to supplement the default values when inserting or loading data.

## Syntax

```plaintext
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
