---
displayed_sidebar: docs
---

# uuid_v7

Returns a time-ordered UUID v7 of the VARCHAR type. UUID v7 is defined in RFC 9562 and provides better database performance compared to random UUIDs (v4) because it maintains temporal ordering, which improves index locality and reduces fragmentation.

The UUID is 36 characters in length and contains 5 hexadecimal numbers connected with four hyphens in the `xxxxxxxx-xxxx-7xxx-xxxx-xxxxxxxxxxxx` format, where the version field is always 7.

This function is non-deterministic. Two calls to this function generate two different UUIDs.

## Syntax

```Haskell
uuid_v7()
```

## Return value

Returns a value of the VARCHAR type.

## Examples

```Plain Text
mysql> SELECT uuid_v7();
+--------------------------------------+
| uuid_v7()                            |
+--------------------------------------+
| 019ba290-15ce-7f13-86b4-ebc97d2f72c0 |
+--------------------------------------+
1 row in set (0.01 sec)
```

## UUID v7 Format

UUID v7 follows RFC 9562 specification with the following structure:

- **48 bits**: Unix timestamp in milliseconds (time-ordered component).
- **4 bits**: Version field, constant value `7`.
- **12 bits**: Random data
- **2 bits**: Variant field (RFC 4122)
- **62 bits**: Random data

This structure ensures that UUIDs generated later will sort after earlier ones, improving index performance.

## Benefits

1. **Time-ordered**: UUIDs generated later will sort after earlier ones.
2. **Improved Index Performance**: Better locality in B-tree indexes.
3. **Reduced Fragmentation**: Sequential inserts cause less page splits.
4. **Unique**: Random bits ensure uniqueness even within the same millisecond.

## Comparison with UUID v4

| Feature | uuid() (v4) | uuid_v7() (v7) |
|---------|-------------|----------------|
| Ordering | Random | Time-ordered |
| Index Performance | Poor | Good |
| Fragmentation | High | Low |
| Timestamp Info | No | Yes (48-bit ms) |
