// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <gen_cpp/Descriptors_types.h>

#include "exec/meta_scan_node.h"
#include "exec/olap_meta_scanner.h"

namespace starrocks {

class RuntimeState;

class OlapMetaScanNode final : public MetaScanNode {
public:
    OlapMetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~OlapMetaScanNode() override = default;
    ;

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

} // namespace starrocks
