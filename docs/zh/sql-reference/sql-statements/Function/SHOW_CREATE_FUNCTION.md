---
displayed_sidebar: docs
---

# SHOW CREATE FUNCTION

SHOW CREATE FUNCTION 返回用户定义函数的 `CREATE FUNCTION` DDL。

## 语法

```SQL
SHOW CREATE [GLOBAL] FUNCTION <function_name> ( <arg_type> [, ...] )
```

## 示例

```Plain
MySQL > SHOW CREATE FUNCTION default_db.python_add(BIGINT);

CREATE FUNCTION default_db.python_add(BIGINT)
RETURNS BIGINT
PROPERTIES (
    "type" = "Python",
    "file" = "inline",
    "symbol" = "add",
    "input" = "arrow"
)
AS $$

import pyarrow.compute as pc

def add(x):
    return pc.add(x, 1)

$$
1 row in set (0.01 sec)
```
