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

#include <vector>

#include "common/statusor.h"
#include "exec/pipeline_node.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {

// Plan node that enforces uniqueness of (file_path, row_position) keys
// across the data stream. Used by MERGE INTO to guarantee that each target
// row is matched by at most one source row.
class EnforceUniqueNode final : public PipelineNode {
public:
    EnforceUniqueNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~EnforceUniqueNode() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    void close(RuntimeState* state) override;

    StatusOr<pipeline::OpFactories> decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;

private:
    std::vector<int32_t> _unique_key_col_indices;
};

} // namespace starrocks
