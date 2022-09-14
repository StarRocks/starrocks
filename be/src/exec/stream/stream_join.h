// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/scan_node.h"

namespace starrocks {
    

class StreamJoinNode final : public starrocks::ExecNode {
public:

    StreamJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs): ExecNode(pool, tnode, descs) {}
    ~StreamJoinNode() override {}
};

} // namespace starrocks