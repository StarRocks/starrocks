---
displayed_sidebar: docs
description: "be_bvars 提供 bRPC 的统计信息，包括 RPC 延迟、QPS 等。"
---

# be_bvars

`be_bvars` 提供有关 bRPC 的统计信息。您可以查看 RPC 延迟、QPS 和某些 StarRocks 组件的统计信息。

`be_bvars` 提供以下字段：

| **字段** | **描述**                          |
| -------- | --------------------------------- |
| BE_ID    | bvar 所在的 BE 的 ID。             |
| NAME     | bvar 名称。                       |
| VALUE    | bvar 导出的值。对于计数器或瞬时值类 bvar，该值为单个数字；对于延迟、QPS 等 recorder 类 bvar，该值为 bvar 为该指标渲染的统计摘要。 |
