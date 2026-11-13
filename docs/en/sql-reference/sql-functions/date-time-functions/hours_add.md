---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Adds hours to a date or datetime."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# hours_add



Adds hours to a date or datetime.

## Syntax

```plaintext
DATETIME hours_add(DATETIME|DATE date, INT hours);
```

## Parameters

`date`: Base date or datetime.

`hours`: Hours to be added.

## Return value

Returns a DATETIME value.

NULL is returned when any of the input parameters is NULL.

## Examples

```Plain Text
select hours_add('2022-01-01 01:01:01', 2);
+-------------------------------------+
| hours_add('2022-01-01 01:01:01', 2) |
+-------------------------------------+
| 2022-01-01 03:01:01                 |
+-------------------------------------+

select hours_add('2022-01-01 01:01:01', -1);
+--------------------------------------+
| hours_add('2022-01-01 01:01:01', -1) |
+--------------------------------------+
| 2022-01-01 00:01:01                  |
+--------------------------------------+

select hours_add('2022-01-01', 1);
+----------------------------+
| hours_add('2022-01-01', 1) |
+----------------------------+
| 2022-01-01 01:00:00        |
+----------------------------+
```
