---
displayed_sidebar: docs
---

# inet_aton



Takes a string containing an IPv4 address in the format A.B.C.D. Returns a BIGINT number representing the corresponding IPv4 address in big endian.

## Syntax

```Haskell
BIGINT inet_aton(VARCHAR ipv4)
```

## Examples

```Plain Text
mysql> select inet_aton('192.168.1.1'); 
+--------------------------------------+ 
| inet_aton('192.168.1.1') | 
+--------------------------------------+ 
| 3232235777                           | 
+--------------------------------------+ 

mysql> select stringIp, inet_aton(stringIp) from ipv4; 
+-----------------+----------------------------+ 
|stringIp         | inet_aton(stringIp)        | 
+-----------------+----------------------------+ 
| 0.0.0.0         | 0                          | 
| 255.255.255.255 | 4294967295                 | 
| invalid         | NULL                       | 
+-----------------+----------------------------+ 
```

## keyword

INET_ATON