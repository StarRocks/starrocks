---
displayed_sidebar: "Chinese"
---

# HLL(HyperLogLog)

## description

向 hll 列中插入数据需要使用 HLL_HASH()函数进行转换，并且 HLL 列只能通过配套的 hll_raw_agg、hll_cardinality、hll_hash 进行查询或使用。详细的使用方法请参考：
[使用 HLL 实现近似去重](../../../using_starrocks/Using_HLL.md)，
[HLL](../data-definition/HLL.md) 章节。

## keyword

HLL,HYPERLOGLOG
