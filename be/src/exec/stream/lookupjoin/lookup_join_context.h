// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <algorithm>
#include <atomic>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/column_ref.h"
#include "runtime/runtime_state.h"
#include "util/blocking_queue.hpp"

namespace starrocks::pipeline {

// Describes about each join key to be used to make conjunct contexts to seek right sides.
struct LookupJoinKeyDesc {
    const TypeDescriptor* left_join_key_type;
    const vectorized::ColumnRef* left_column_ref;
    const vectorized::ColumnRef* right_column_ref;
};

// All params which are used in lookup join operators.
struct LookupJoinContextParams {
    LookupJoinContextParams(uint32_t left_dop, uint32_t right_dop, const std::vector<LookupJoinKeyDesc>& join_key_descs,
                            const RowDescriptor& left_row_desc, const RowDescriptor& right_row_desc,
                            const std::vector<ExprContext*>& other_join_conjunct_ctxs)
            : left_dop(left_dop),
              right_dop(right_dop),
              join_key_descs(join_key_descs),
              left_row_desc(left_row_desc),
              right_row_desc(right_row_desc),
              other_join_conjunct_ctxs(other_join_conjunct_ctxs) {}
    uint32_t left_dop;
    uint32_t right_dop;
    const std::vector<LookupJoinKeyDesc>& join_key_descs;
    const RowDescriptor& left_row_desc;
    const RowDescriptor& right_row_desc;
    const std::vector<ExprContext*>& other_join_conjunct_ctxs;
};

// Context which bridges between lookupjoin's left and right sides.
class LookupJoinContext final : public ContextWithDependency {
public:
    explicit LookupJoinContext(LookupJoinContextParams params) : _chunks(params.left_dop), _params(params) {}

    void close(RuntimeState* state) override {}
    bool is_ready() { return !_chunks.empty() || is_finished(); }
    bool blocking_put(const vectorized::ChunkPtr& chunk) { return _chunks.blocking_put(chunk); }
    int try_get(vectorized::ChunkPtr* result) { return _chunks.try_get(result); }
    const std::vector<LookupJoinKeyDesc>& join_key_descs() { return _params.join_key_descs; }
    const LookupJoinContextParams& params() { return _params; }
    bool is_finished() const { return _is_finished.load(std::memory_order_acquire) && _chunks.empty(); }
    void mark_finished(bool val) {
        _chunks.shutdown();
        _is_finished.store(val);
    }

private:
    // Chunks which are sent by left sides and will be consumed by right sides.
    BlockingQueue<vectorized::ChunkPtr> _chunks;
    LookupJoinContextParams _params;
    std::atomic_bool _is_finished = false;
};

} // namespace starrocks::pipeline
