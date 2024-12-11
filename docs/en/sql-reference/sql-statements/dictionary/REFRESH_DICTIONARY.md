---
displayed_sidebar: docs
---

# REFRESH DICTIONARY



Manually refreshes a dictionary object. Internally, the system will query the latest data from the original object and write it into the dictionary object.

## Syntax

```SQL
REFRESH DICTIONARY <dictionary_object_name>
```

## Parameters

- **dictionary_object_name**: The name of the dictionary object.

## Examples

Manually refresh the dictionary object `dict_obj`.

```Plain
MySQL > REFRESH DICTIONARY dict_obj;
Query OK, 0 rows affected (0.01 sec)
```