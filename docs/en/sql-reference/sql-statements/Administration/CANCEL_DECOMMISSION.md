---
displayed_sidebar: "English"
---

# CANCEL DECOMMISSION

## Description

This statement is used to undo a node decommission. (Administrator only!)

Syntax:

```sql
CANCEL DECOMMISSION BACKEND "<host>:<heartbeat_port>"[,"<host>:<heartbeat_port>"...]
```

## Examples

1. Cancel decommission of two nodes.

    ```sql
    CANCEL DECOMMISSION BACKEND "host1:port", "host2:port";
    ```
