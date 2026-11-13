---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Converts a time value into the number of seconds."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# time_to_sec



Converts a time value into the number of seconds. The formula used for the conversion is as follows:

Hour x 3600 + Minute x 60 + Second

## Syntax

```plaintext
BIGINT time_to_sec(TIME time)
```

## Parameters

`time`: It must be of the TIME type.

## Return value

Returns a value of the BIGINT type. If the input is invalid, NULL is returned.

## Examples

```plain text
select time_to_sec('12:13:14');
+-----------------------------+
| time_to_sec('12:13:14')     |
+-----------------------------+
|                        43994|
+-----------------------------+
```
