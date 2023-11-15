# BITMAP

## description

描述：

BITMAP与HLL类似只能作为聚合表的value类型使用，常见用来加速count distinct的去重计数使用，

通过位图可以进行精确计数，可以通过bitmap函数可以进行集合的各种操作，相对HLL他可以获得精确的结果，

但是需要消耗更多的内存和磁盘资源，另外Bitmap只能支持整数类型的聚合，如果是字符串等类型需要采用字典进行映射。

## keyword

BITMAP BITMAP_UNION
