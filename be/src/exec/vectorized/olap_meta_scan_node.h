// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include <gen_cpp/Descriptors_types.h>

#include "exec/vectorized/meta_scan_node.h"
#include "exec/vectorized/olap_meta_scanner.h"

namespace starrocks {

class RuntimeState;

namespace vectorized {

class OlapMetaScanNode final : public MetaScanNode {
public:
    OlapMetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~OlapMetaScanNode() override = default;;

    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;

    void debug_string(int indentation_level, std::stringstream* out) const override {
        *out << "vectorized:OlapMetaScanNode";
    }

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) override;

private:
    friend class OlapMetaScanner;
    std::vector<OlapMetaScanner*> _scanners;
};

} // namespace vectorized
} // namespace starrocks
