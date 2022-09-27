// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/scan_node.h"

namespace starrocks {

class StreamScanNode final : public starrocks::ExecNode {
public:
    StreamScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : ExecNode(pool, tnode, descs) {}
    ~StreamScanNode() override {}
};

} // namespace starrocks