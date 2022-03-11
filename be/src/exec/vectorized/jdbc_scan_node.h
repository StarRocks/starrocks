// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/olap_common.h"
#include "exec/scan_node.h"
#include "runtime/descriptors.h"

namespace starrocks {

class TupleDescriptor;
class RuntimeState;
class MemPool;
class Status;
} // namespace starrocks

namespace starrocks::vectorized {
class JDBCScanNode final : public ScanNode {
public:
    JDBCScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~JDBCScanNode() override {
        if (runtime_state() != nullptr) {
            close(runtime_state());
        }
    }

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;

    Status close(RuntimeState* state) override;

    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

    pipeline::OpFactories decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;

protected:
    void debug_string(int indentation_level, std::stringstream* out) const override;

private:
    TJDBCScanNode _jdbc_scan_node;
    std::unique_ptr<MemPool> _tuple_pool;
};
} // namespace starrocks::vectorized