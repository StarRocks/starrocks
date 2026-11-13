---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Cancels the refresh of a dictionary object."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# CANCEL REFRESH DICTIONARY



Cancels the refresh of a dictionary object.

## Syntax

```SQL
CANCEL REFRESH DICTIONARY <dictionary_object_name>
```

## Parameters

- **dictionary_object_name**: The name of the dictionary object that is in the REFRESHING state.

## Examples

Cancel the refresh of the dictionary object `dict_obj`.

```Plain
MySQL > CANCEL REFRESH DICTIONARY dict_obj;
Query OK, 0 rows affected (0.01 sec)
```