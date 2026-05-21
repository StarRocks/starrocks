---
displayed_sidebar: docs
---

# SHOW CREATE FUNCTION

SHOW CREATE FUNCTION は、ユーザー定義関数の `CREATE FUNCTION` DDL を返します。

## 構文

```SQL
SHOW CREATE [GLOBAL] FUNCTION <function_name> ( <arg_type> [, ...] )
```

## 例

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
