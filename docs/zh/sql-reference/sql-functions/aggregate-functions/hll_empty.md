---
displayed_sidebar: "Chinese"
---

# hll_empty

## 功能

生成空 HLL 列，用于 INSERT 或导入数据时补充默认值。

## 语法

```Haskell
HLL_EMPTY()
```

## 参数说明

无

## 返回值说明

返回值为空的 HLL。

## 示例

示例一：INSERT 时补充默认值。

```plain text
insert into hllDemo(k1,v1) values(10,hll_empty());
```

示例二：导入数据时补充默认值。

```plain text
curl --location-trusted -u <username>:<password> \
    -H "columns: temp1, temp2, col1=hll_hash(temp1), col2=hll_empty()" \
    -T example7.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table7/_stream_load
```
