---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Manually refreshes a dictionary object."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
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