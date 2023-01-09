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

#include <unordered_set>

#include "column/chunk.h"
#include "column/column_hash.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "exec/olap_common.h"
#include "exprs/expr_context.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "util/hash_util.hpp"
#include "util/phmap/phmap.h"
#include "util/slice.h"

namespace starrocks {

// Node for assert row count
class AssertNumRowsNode final : public ExecNode {
public:
    AssertNumRowsNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~AssertNumRowsNode() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    Status close(RuntimeState* state) override;

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

private:
    int64_t _desired_num_rows;
    const std::string _subquery_string;
    TAssertion::type _assertion;
    std::deque<ChunkPtr> _input_chunks;
    bool _has_assert;
};

} // namespace starrocks
