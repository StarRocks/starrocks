---
displayed_sidebar: docs
---

# get_query_dump

`get_query_dump(query)`
`get_query_dump(query, enable_mock)`

These functions return a dump of the query for debugging purposes.

## Arguments

`query`: The SQL query string (VARCHAR).
`enable_mock`: (Optional) A boolean value indicating whether to enable mock data for the dump. Defaults to `FALSE`.

## Return Value

Returns a VARCHAR string containing the query dump.

