---
displayed_sidebar: docs
---

# get_query_dump

`get_query_dump(query)`
`get_query_dump(query, enable_mock)`

这些函数返回用于调试目的的查询转储。

## 参数

`query`: SQL 查询字符串 (VARCHAR)。
`enable_mock`: (可选) 一个布尔值，指示是否为转储启用模拟数据。默认为 `FALSE`。

## 返回值

返回包含查询转储的 VARCHAR 字符串。

