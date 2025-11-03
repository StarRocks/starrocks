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

#include "exec/exec_node.h"
#include "exec/pipeline/set/raw_values_source_operator.h"
#include "runtime/types.h"

namespace starrocks {

// RawValuesNode is optimized for large constant lists.
// It avoids expensive expression evaluation by directly constructing
// columns from typed raw data (List<Long> or List<String>).
class RawValuesNode final : public ExecNode {
public:
    RawValuesNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~RawValuesNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    pipeline::OpFactories decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;

private:
    const int _tuple_id;
    const TupleDescriptor* _tuple_desc = nullptr;

    TypeDescriptor _constant_type;
    std::vector<int64_t> _long_values;
    std::vector<std::string> _string_values;
};

} // namespace starrocks
