# 其他常见问题

## 建表时，varchar(32)和string占用的存储空间是否相同？查询时的性能是否相同？

都是变长存储的，查询性能一样。

## Oracle导出的txt文件，修改文件的字符集utf-8后还是乱码，如何处理？

可以尝试将文件字符集视为gbk进行字符集转换。
以文件“origin.txt”为例，假设用命令查看其字符集得到其当前字符集为iso-8859-1：

```plain text
file --mime-encoding origin.txt
返回值[假设]：iso-8859-1
```

使用iconv命令转换，将文件字符集转换为utf-8：

```shell
iconv -f iso-8859-1 -t utf-8 origin.txt > origin_utf-8.txt
```

若此时发现转换后得到的origin_utf-8.txt文件中还存在乱码，我们就可以将origin.txt的原字符集视为gbk，重新转换：

```shell
iconv -f gbk -t utf-8 origin.txt > origin_utf-8.txt
```

## MySQL中定义字符串长度跟StarRocks定义的是否一致？

目前StarRocks中varchar(n)，n限制的是字节数，MySQL限制的是字符数，所以对应MySQL那边的表，n可以给3倍或者4倍，一般也不会占更多存储。

## 表的分区字段是否可以使用float、double、decimal浮点数类型来分区？

不可以，只能是date、datetime或int整型。

## 如何查看表中的数据占了多大的存储？

`show data`可以看到，可展示数据量、副本数量以及统计行数。注意数据统计，有一定的时间延迟。

## 如果数据超过了这个quota的量会怎么样？这个值可以做更改吗？

```sql
ALTER DATABASE example_db SET DATA QUOTA 10T;
```

改动db的quota，调整这个db的容量上限。
