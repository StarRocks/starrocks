---
displayed_sidebar: "English"
---

# CANCEL EXPORT

## Description

Cancels an Export job with a specified query ID.

## Syntax

```sql
CANCEL EXPORT
[FROM db_name]
WHERE QUERYID = "your_query_id";
```

## Examples

1. Cancel the Export job whose query ID is `921d8f80-7c9d-11eb-9342-acde48001122` in the database `example_db`.

    ```sql
    CANCEL EXPORT FROM example_db WHERE queryid = "921d8f80-7c9d-11eb-9342-acde48001122";
    ```

## keywords

CANCEL, EXPORT
