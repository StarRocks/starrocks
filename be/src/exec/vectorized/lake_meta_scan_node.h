// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <gen_cpp/Descriptors_types.h>

#include "exec/vectorized/meta_scan_node.h"
#include "exec/vectorized/lake_meta_scanner.h"

namespace starrocks {
namespace vectorized {

class LakeMetaScanNode final : public MetaScanNode {
public:
    LakeMetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~LakeMetaScanNode() override = default;

    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;

    void debug_string(int indentation_level, std::stringstream* out) const override {
        *out << "vectorized:LakeMetaScanNode";
    }

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) override;

private:
    friend class LakeMetaScanner; 
    std::vector<LakeMetaScanner*> _scanners;
};

} // namespace vectorized
} // namespace starrocks
