---
displayed_sidebar: "English"
---

# time_to_sec

## Description

Converts a time value into the number of seconds. The formula used for the conversion is as follows:

Hour x 3600 + Minute x 60 + Second

## Syntax

```Haskell
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
