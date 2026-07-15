---
displayed_sidebar: docs
sidebar_label: "内存与落盘"
sidebar_position: 4
description: "控制查询内存限制以及溢出到磁盘或远程存储的会话变量。"
---

# 系统变量 - 内存与落盘

如需了解如何查看和设置变量，请参见 [系统变量概述](../System_variable.md)。

### disable_spill_to_local_disk

* **描述**: 当会话中设置为 `true` 时，FE 将指示 BE 禁用本地磁盘落盘（spilling to local disk），并改为依赖远程存储落盘（如果配置了远程溢写）。该标志仅在 `enable_spill` 为 `true`、`enable_spill_to_remote_storage` 为 `true` 且 FE 提供并找到有效的 `spill_storage_volume` 时才有意义。如果未配置远程溢写或无法解析所命名的存储卷，则此设置无效。谨慎使用：禁用本地磁盘落盘可能会增加网络 I/O 和延迟，并要求远程存储具有可靠性和良好性能。
* **范围**: Session
* **默认值**: `false`
* **数据类型**: boolean
* **引入版本**: v3.3.0, v3.4.0, v3.5.0

### enable_spill

* 描述：是否启用中间结果落盘。默认值：`false`。如果将其设置为 `true`，StarRocks 会将中间结果落盘，以减少在查询中处理聚合、排序或连接算子时的内存使用量。
* 默认值：false
* 引入版本：v3.0

### enable_spill_to_remote_storage

* 描述：是否启用将中间结果落盘至对象存储。如果设置为 `true`，当本地磁盘的用量达到上限后，StarRocks 将中间结果落盘至 `spill_storage_volume` 中指定的存储卷中。有关更多信息，请参阅 [将中间结果落盘至对象存储](../administration/management/resource_management/spill_to_disk.md#preview-将中间结果落盘至对象存储)。
* 默认值：false
* 引入版本：v3.3.0

### query_mem_limit

* 描述：用于设置每个 BE 节点上单个查询的内存限制。该项仅在启用 Pipeline Engine 后生效。设置为 `0` 表示没有限制。
* 单位：字节
* 默认值：`0`

### spill_encode_level

* **范围**: Session
* **描述**: 控制应用于 operator 溢写（spill）文件的编码/压缩行为。该整数为位标志级别，其含义与 `transmission_encode_level` 对应：
  * bit 1 (值 `1`) — 启用自适应编码；
  * bit 2 (值 `2`) — 对类似整数的列使用 streamvbyte 编码；
  * bit 4 (值 `4`) — 对二进制/字符串列使用 LZ4 压缩。
  来自相关 `transmission_encode_level` 注释的示例语义：`7` 为数字和字符串启用自适应编码；`6` 强制对数字和字符串进行编码。更改此值会调整溢写时的 CPU 与磁盘 I/O 权衡（更高的编码级别会增加 CPU 工作但减少溢写大小 / I/O）。
  作为会话变量在 `SessionVariable.java` 中以注释 `SPILL_ENCODE_LEVEL` 实现（getter 为 `getSpillEncodeLevel()`），并与其他溢写可调项（例如 `spill_mem_table_size`）相邻记录。
* **默认值**: `7`
* **类型**: int
* **引入版本**: v3.2.0

### spill_mode (3.0 及以后)

中间结果落盘的执行方式。默认值：`auto`。有效值包括：

* `auto`：达到内存使用阈值时，会自动触发落盘。
* `force`：无论内存使用情况如何，StarRocks 都会强制落盘所有相关算子的中间结果。

此变量仅在变量 `enable_spill` 设置为 `true` 时生效。

### spill_storage_volume

* 描述：用于存储触发落盘的查询的中间结果的存储卷。有关更多信息，请参阅 [将中间结果落盘至对象存储](../administration/management/resource_management/spill_to_disk.md#preview-将中间结果落盘至对象存储)。
* 默认值：空字符串
* 引入版本：v3.3.0

