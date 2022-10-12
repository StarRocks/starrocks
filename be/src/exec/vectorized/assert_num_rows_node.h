// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

namespace starrocks::vectorized {

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

} // namespace starrocks::vectorized
