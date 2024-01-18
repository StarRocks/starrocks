---
displayed_sidebar: "Chinese"
---

# 导入通用常见问题

## 1. 发生 "close index channel failed" 和 "too many tablet versions" 错误应该如何处理？

上述报错是因为导入频率太快，数据没能及时合并 (Compaction) ，从而导致版本数超过支持的最大未合并版本数。默认支持的最大未合并版本数为 1000。可以通过如下方法解决上述报错：

- 增大单次导入的数据量，降低导入频率。

- 在 BE 的配置文件 **be.conf** 中修改以下配置，通过调整合并策略实现加快合并的目的：

    ```Plain
    cumulative_compaction_num_threads_per_disk = 4
    base_compaction_num_threads_per_disk = 2
    cumulative_compaction_check_interval_seconds = 2
    ```

  修改完成后，需要观察内存和 I/O，确保内存和 I/O 正常。

## 2. 发生 "Label Already Exists" 错误应该如何处理？

StarRocks 集群中同一个数据库内已经有一个具有相同标签的导入作业执行成功或正在执行。原因如下：

由于 Stream Load 是采用 HTTP 协议提交导入作业的请求，一般各个语言的 HTTP 客户端均会自带请求重试逻辑。StarRocks 集群在接受到第一个请求后，已经开始操作 Stream Load，但是由于没有及时向客户端返回结果，客户端会发生再次重试发送相同请求的情况。这时候 StarRocks 集群由于已经在操作第一个请求，所以第二个请求会返回 `Label Already Exists` 的状态提示。

需要检查不同导入方式之间是否有标签冲突、或是有重复提交的导入作业。排查方法如下：

- 使用标签搜索主 FE 的日志，看是否存在同一个标签出现了两次的情况。如果有，就说明客户端重复提交了该请求。

  > **说明**
  >
  > StarRocks 集群中导入作业的标签不区分导入方式。因此，可能存在不同的导入作业使用了相同标签的问题。

- 运行 SHOW LOAD WHERE LABEL = "xxx" 语句，查看是否已经存在具有标签相同、且处于 **FINISHED** 状态的导入作业。

  > **说明**
  >
  > 其中 `xxx` 为待检查的标签字符串。

建议根据当前请求导入的数据量，计算出大致的导入耗时，并根据导入超时时间来适当地调大客户端的请求超时时间，从而避免客户端多次提交该请求。

## 3. 发生数据质量错误 "ETL_QUALITY_UNSATISFIED; msg:quality not good enough to cancel" 应该如何处理？

运行 [SHOW LOAD](../../sql-reference/sql-statements/data-manipulation/SHOW_LOAD.md) 语句。在语句返回的信息中，找到 URL，然后查看错误数据。

常见的数据质量错误有：

- "convert csv string to INT failed."
  
  源数据文件中某列的字符串在转化为对应类型的数据时出错。比如，将 `abc` 转化为数字时失败。

- "the length of input is too long than schema."
  
  源数据文件中某列的长度不正确。比如定长字符串超过建表设置的长度，或 INT 类型的字段超过 4 个字节。

- "actual column number is less than schema column number."
  
  源数据文件中某一行按照指定的分隔符切分后，列数小于指定的列数。可能原因是分隔符不正确。

- "actual column number is more than schema column number."
  
  源数据文件中某一行按照指定的分隔符切分后，列数大于指定的列数。

- "the frac part length longer than schema scale."
  
  源数据文件中某 DECIMAL 类型的列的小数部分超过指定的长度。

- "the int part length longer than schema precision."
  
  源数据文件中某 DECIMAL 类型的列的整数部分超过指定的长度。

- "there is no corresponding partition for this key."
  
  源数据文件中某行的分区列的值不在分区范围内。

## 4. 导入过程中，发生 RPC 超时应该如何处理？

检查 BE 配置文件 **be.conf** 中 `write_buffer_size` 参数的设置。该参数用于控制 BE 上内存块的大小阈值，默认阈值为 100 MB。如果阈值过大，可能会导致远程过程调用（Remote Procedure Call，简称 RPC）超时，这时候需要配合 BE 配置文件中的 `tablet_writer_rpc_timeout_sec` 参数来适当地调整 `write_buffer_size` 参数的取值。请参见 [BE 配置](../../loading/Loading_intro.md#be-配置)。

## 5. 导入作业报错 "Value count does not match column count" 应该怎么处理？

导入作业失败，通过查看错误详情 URL 发现返回 "Value count does not match column count" 错误，提示解析源数据得到的列数与目标表的列数不匹配：

```Java
Error: Value count does not match column count. Expect 3, but got 1. Row: 2023-01-01T18:29:00Z,cpu0,80.99
Error: Value count does not match column count. Expect 3, but got 1. Row: 2023-01-01T18:29:10Z,cpu1,75.23
Error: Value count does not match column count. Expect 3, but got 1. Row: 2023-01-01T18:29:20Z,cpu2,59.44
```

发生该错误的原因是导入命令或导入语句中指定的列分隔符与源数据中的列分隔符不一致。例如上面示例中，源数据为 CSV 格式，包括三列，列分隔符为逗号 (`,`)，但是导入命令或导入语句中却指定制表符 (`\t`) 作为列分隔符，最终导致源数据的三列数据解析成了一列数据。

修改导入命令或导入语句中的列分隔符为逗号 (`,`)，然后再次尝试执行导入。
