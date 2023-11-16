# Stream Load常见问题

## Stream Load是否可识别文本文件中首行的列名？或者能否指定不读取第一行？

Stream Load不能识别文本中首行的列名，首行对Stream Load来说也只是普通数据。当前还不能指定不读取首行，若需要导入的文件首行为列名，可以使用如下四种方式处理：

1. 在导出工具中修改设置，重新导出不带列名的文本类数据文件。
2. 使用sed -i '1d' filename等命令删除文本类文件首行。
3. 在Stream Load语句中使用`-H "where: 列名 != '列名称'"`把该行过滤掉。首行字符串转其他类型的转不过去就会返回 null，所以该种方式要求表中字段不能设置的有not null。
4. 在Stream Load命令中加入`-H "max_filter_ratio:0.01"`，根据数据量给它一个“1%或者更小但能容错超过1行的容错率”，将首行的错误忽视掉。加入容错率后，返回结果的ErrorURL依旧会提示有错误，但任务整体会成功。容错率不宜设置过大，避免漏掉其他数据问题。

## 当前业务的分区键对应的数据不是标准的date和int，比如是202106.00 的格式，假如需要使用Stream Load导入到StarRocks中，要如何转换？

StarRocks支持在导入过程中进行数据转换，具体可以参考企业版文档“4.7 导入过程中完成数据转换”。

以Stream Load为例，假设表TEST中有NO、DATE、VERSION、PRICE四列，导出的CSV数据文件中DATE字段内容是不规范的202106.00格式。如果在StarRocks中需使用的分区列为DATE，那么首先我们需要在StarRocks中进行建表，指定DATE类型为date、datetime或int。之后，在Stream Load命令中，使用：

```plain text
-H "columns: NO,DATE_1, VERSION, PRICE, DATE=LEFT(DATE_1,6)"
```

<<<<<<< HEAD
来实现列的转换。DATE_1可以简单的认为是先占位进行取数，然后通过函数转换，赋值给StarRocks中对应的字段。特别注意，我们需要先列出 CSV 数据文件中的所有列，再进行函数转换，常规函数这里都可以使用。
=======
`DATE_1` 可以简单地看成是先占位进行取数，然后通过 left() 函数进行转换，赋值给 StarRocks 表中的 `DATE` 列。特别需要注意的是，必须先列出 CSV 文件中所有列的临时名称，然后再使用函数进行转换。支持列转换的函数为标量函数，包括非聚合函数和窗口函数。

## 3. 数据质量问题报错 "ETL_QUALITY_UNSATISFIED; msg:quality not good enough to cancel" 应该怎么解决？

请参见[导入通用常见问题](./Loading_faq.md)。

## 4. 导入状态为 "Label Already Exists" 应该怎么解决？

请参见[导入通用常见问题](./Loading_faq.md)。

## 5. 导入出错 "body exceed max size: 10737418240, limit: 10737418240" 应该如何解决？

导入文件大小超过 10GB, 超过 Stream Load 所能支持的文件大小上限。有两种解决方法:

1. 把文件通过 `seq -w 0 n` 拆分。
2. 通过 `curl -XPOST http:///be_host:http_port/api/update_config?streaming_load_max_mb=1024000` 来扩大这个上限。
>>>>>>> a83aa885d ([Doc] fix links in 2.2 (#35221))
