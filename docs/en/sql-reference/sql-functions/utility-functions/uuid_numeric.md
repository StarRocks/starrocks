---
displayed_sidebar: docs
---

# uuid_numeric



Returns a random UUID of the LARGEINT type. This function has an execution performance 2 orders of magnitude better than the `uuid` function.

## Syntax

```Haskell
uuid_numeric();
```

## Parameters

None

## Return value

Returns a value of the LARGEINT type.

## Examples

```Plain Text
MySQL > select uuid_numeric();
+--------------------------+
| uuid_numeric()           |
+--------------------------+
| 558712445286367898661205 |
+--------------------------+
1 row in set (0.00 sec)
```

## See also

- [uuid](./uuid.md): Returns a random UUID as VARCHAR
- [uuid_v7](./uuidv7.md): Returns a time-ordered UUID v7
- [uuid_v7_numeric](./uuid_v7_numeric.md): Returns a time-ordered UUID v7 as LARGEINT
