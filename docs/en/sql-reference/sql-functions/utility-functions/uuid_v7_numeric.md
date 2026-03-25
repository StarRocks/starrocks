---
displayed_sidebar: docs
---

# uuid_v7_numeric

Returns a time-ordered UUID v7 of the LARGEINT type. UUID v7 is defined in RFC 9562 and provides better database performance compared to random UUIDs (v4) because it maintains temporal ordering, which improves index locality and reduces fragmentation.

This function returns the UUID v7 as a 128-bit integer instead of a string format, which can be more efficient for storage and comparison operations.

This function is non-deterministic. Two calls to this function generate two different UUIDs.

## Syntax

```Haskell
uuid_v7_numeric()
```

## Return value

Returns a value of the LARGEINT type (128-bit integer).

## Examples

```Plain Text
mysql> SELECT uuid_v7_numeric();
+---------------------------------------+
| uuid_v7_numeric()                     |
+---------------------------------------+
| 2088748395792837468371928374619283746 |
+---------------------------------------+
1 row in set (0.01 sec)
```

## Benefits

1. **Time-ordered**: UUID values generated later will be larger than earlier ones.
2. **Improved Index Performance**: Better locality in B-tree indexes.
3. **Reduced Fragmentation**: Sequential inserts cause fewer page splits.
4. **Unique**: Random bits ensure uniqueness even within the same millisecond.
5. **Efficient Storage**: Numeric representation can be more compact than string format.

## Comparison with uuid_numeric

| Feature | uuid_numeric() (v4) | uuid_v7_numeric() (v7) |
|---------|---------------------|------------------------|
| Ordering | Random | Time-ordered |
| Index Performance | Poor | Good |
| Fragmentation | High | Low |
| Timestamp Info | No | Yes (48-bit ms) |
