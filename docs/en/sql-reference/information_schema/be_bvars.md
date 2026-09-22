---
displayed_sidebar: docs
description: "be_bvars provides statistical information regarding bRPC."
---

# be_bvars

`be_bvars` provides statistical information regarding bRPC. You can view RPC latency, QPS, and other statistics for some components of StarRocks.

The following fields are provided in `be_bvars`:

| **Field** | **Description**                                              |
| --------- | ------------------------------------------------------------ |
| BE_ID     | ID of the BE where the bvar is located.                      |
| NAME      | Name of the bvar.                                            |
| VALUE     | Dumped value of the bvar. For a simple counter or gauge this is a single number; for a recorder such as a latency or QPS bvar it is the statistical summary that bvar renders for that metric. |
