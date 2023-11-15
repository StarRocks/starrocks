# Stream Load 常见问题

## 1. Stream Load 是否支持识别 CSV 格式文件开头前几行的列名？或者是否支持在数据读取过程中跳过开头前几行数据？

Stream Load 不支持识别 CSV 格式文件开头前几行的列名，列名对 Stream Load 来说跟其他行一样，只是普通数据。

在 2.5 及以前版本，Stream Load 不支持在数据读取过程中跳过 CSV 格式文件开头前几行数据。如果需要导入的 CSV 格式文件开头前几行为列名，可以使用如下四种方式处理：

- 在导出工具中修改设置，重新导出不带列名的 CSV 格式文件。

- 使用 `sed -i '1d' filename` 等命令删除 CSV 格式文件的前几行。

- 在 Stream Load 执行命令或语句中，使用 `-H "where: <column_name> != '<column_name>'"` 把前几行过滤掉。其中，`<column_name>` 是 CSV 格式文件开头前几行里的任意一个列名。需要注意的是，当前 StarRocks 会先转换、然后再做过滤，因此如果前几行中的列值转其他数据类型失败的话，会返回 `NULL`。所以，这种导入方式要求 StarRocks 表中的列不能有设置为 `NOT NULL` 的。

- 在Stream Load 执行命令或语句中加入 `-H "max_filter_ratio:0.01"`，这样可以给导入作业设置一个 1% 或者更小、但能容错数行的容错率，从而将前几行的错误忽视掉。设置容错率后，返回结果的 `ErrorURL` 依旧会提示有错误，但导入作业整体会成功。容错率不宜设置过大，避免漏掉其他数据问题。

从 3.0 版本起，Stream Load 支持 `skip_header` 参数，用于指定跳过 CSV 文件开头的前几行数据。参见 [CSV 适用参数](../../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md#csv-适用参数)。

## 2. 当前业务的分区键对应的数据不是标准的 DATE 和 INT 类型，比如是 202106.00 的格式，如果需要使用 Stream Load 把这些数据导入到 StarRocks 中，需要如何转换？

StarRocks 支持在导入过程中进行数据转换，具体请参见[导入过程中完成数据转换](/loading/Etl_in_loading.md)。

假设源数据文件 `TEST` 为 CSV 格式，并且包含 `NO`、`DATE`、`VERSION`、`PRICE` 四列，但是其中 `DATE` 列是不规范的 202106.00 格式。如果在 StarRocks 中需使用的分区列为 `DATE`，那么首先需要在 StarRocks 中创建一张表，这里假设 StarRocks 表包含 `NO`、`VERSION`、`PRICE`、`DATE` 四列。您需要指定 `DATE` 列的数据类型为 DATE、DATETIME 或 INT。然后，在 Stream Load 执行命令或者语句中，通过指定如下设置来实现列之间的转换：

```Plain
-H "columns: NO,DATE_1, VERSION, PRICE, DATE=LEFT(DATE_1,6)"
```

`DATE_1` 可以简单地看成是先占位进行取数，然后通过 `left()` 函数进行转换，赋值给 StarRocks 表中的 `DATE` 列。特别需要注意的是，必须先列出 CSV 文件中所有列的临时名称，然后再使用函数进行转换。支持列转换的函数为标量函数，包括非聚合函数和窗口函数。

## 3. 导入出错 "body exceed max size: 10737418240, limit: 10737418240" 应该如何解决？

源数据文件大小超过 10 GB, 超过 Stream Load 所能支持的文件大小上限。有两种解决方法：

- 通过 `seq -w 0 n` 拆分数据文件。
- 通过 `curl -XPOST http:///be_host:http_port/api/update_config?streaming_load_max_mb=<file_size>` 调整 [BE 配置项](../../administration/Configuration.md#配置-be-动态参数) `streaming_load_max_mb` 的取值来扩大文件大小上限。
