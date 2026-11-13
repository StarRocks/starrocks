---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "SHOW FULL COLUMNS displays detailed column information for specified tables."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# SHOW FULL COLUMNS

## 功能

该语句用于查看指定表的列信息。

:::tip

该操作不需要权限。

:::

## 语法

```sql
SHOW FULL COLUMNS FROM <tbl_name>
```

## 示例

1. 查看指定表的列信息。

    ```sql
    SHOW FULL COLUMNS FROM tbl;
    ```
