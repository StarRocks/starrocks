---
displayed_sidebar: docs
---

# inet_aton

IPv4 アドレスを A.B.C.D 形式で含む文字列を受け取り、対応する IPv4 アドレスをビッグエンディアンで表す BIGINT 数値を返します。

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