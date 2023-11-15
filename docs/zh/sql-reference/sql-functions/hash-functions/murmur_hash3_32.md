# murmur_hash3_32

## description

### Syntax

`INT MURMUR_HASH3_32(VARCHAR input, ...)`

返回输入字符串的32位murmur3 hash值

## example

```Plain Text
MySQL > select murmur_hash3_32(null);
+-----------------------+
| murmur_hash3_32(NULL) |
+-----------------------+
|                  NULL |
+-----------------------+

MySQL > select murmur_hash3_32("hello");
+--------------------------+
| murmur_hash3_32('hello') |
+--------------------------+
|               1321743225 |
+--------------------------+

MySQL > select murmur_hash3_32("hello", "world");
+-----------------------------------+
| murmur_hash3_32('hello', 'world') |
+-----------------------------------+
|                         984713481 |
+-----------------------------------+
```

## keyword

MURMUR_HASH3_32,HASH
