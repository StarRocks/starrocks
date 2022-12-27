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

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/stream_chunk.h"
#include "exec/pipeline/operator.h"

namespace starrocks {

class StreamJoinNode final : public starrocks::ExecNode {
public:
    StreamJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : ExecNode(pool, tnode, descs) {}
    ~StreamJoinNode() override {}

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    pipeline::OpFactories decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;

private:
    TJoinOp::type _join_op;
    std::string _sql_join_conjuncts;
    std::vector<ExprContext*> _probe_expr_ctxs;
    std::vector<ExprContext*> _build_expr_ctxs;
    std::vector<ExprContext*> _other_join_conjunct_ctxs;

    std::set<SlotId> _output_slots;
    RowDescriptor _left_row_desc;
    RowDescriptor _right_row_desc;

    // std::vector<pipeline::LookupJoinKeyDesc> _join_key_descs;
    // std::shared_ptr<pipeline::LookupJoinContext> _lookup_join_context;
};

} // namespace starrocks
