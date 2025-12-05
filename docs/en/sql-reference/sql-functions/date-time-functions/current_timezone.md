---
displayed_sidebar: docs
---

# current_timezone



Obtains the current time zone and returns a value of the VARCHAR type.

## Syntax

```Haskell
VARCHAR CURRENT_TIMETIME()
```

## Examples

```Plain Text
MySQL > select current_timezone();
+---------------------+
| current_timezone()  |
+---------------------+
| America/Los_Angeles |
+---------------------+
```