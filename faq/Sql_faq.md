# 查询相关问题

## 构建物化视图失败：fail to allocate memory

修改 be.conf 中的`memory_limitation_per_thread_for_schema_change`。

该参数表示单个schema change任务允许占用的最大内存，默认大小2G。

## StarRocks对结果缓存这块有限制吗？

starrocks不会对结果缓存，第一次查询慢后面快的原因是因为后续的查询使用了操作系统的 pagecache。

pagecache大小可以通过设置 be.conf 中`storage_page_cache_limit`参数来限制pagecache，默认20G。

## 当字段为NULL时，除了is null， 其他所有的计算结果都是false

标准sql中null和其他表达式计算结果都是null。
