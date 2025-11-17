---
displayed_sidebar: docs
---

# sec_to_time

## Description

The SEC_TO_TIME function converts a value in seconds into a TIME type, returning the result in the format HH:MM:SS.
The input seconds represent the time elapsed since the start of a day (00:00:00).

## Syntax

```Haskell
TIME sec_to_time(BIGINT sec)
```

## Parameters

`sec`: It must be of the INT type.

## Return value

Returns a TIME value in the format HH:MM:SS, representing the time calculated from the start of a day (00:00:00).
If sec is NULL, the function returns NULL.

## Examples

```plain text
select sec_to_time(43994);
+-----------------------------+
| sec_to_time(43994)          |
+-----------------------------+
|                   '12:13:14'|
+-----------------------------+
```

