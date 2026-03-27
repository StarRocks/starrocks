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

#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/pipeline_node.h"
#include "exprs/expr.h"
#include "exprs/table_function/table_function_factory.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state_fwd.h"

namespace starrocks {
class TableFunctionNode final : public PipelineNode {
public:
    TableFunctionNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& desc);

    ~TableFunctionNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status reset(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    StatusOr<pipeline::OpFactories> decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;

private:
    const TPlanNode& _tnode;
    const TableFunction* _table_function = nullptr;

    //Slots of table function input parameters
    std::vector<SlotId> _param_slots;

    //table function param and return offset
    TableFunctionState* _table_function_state = nullptr;
};

} // namespace starrocks
