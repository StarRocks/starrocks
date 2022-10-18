// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>

#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks {
struct StateAllocator;
class SortedStreamingAggregator final : public Aggregator {
public:
    SortedStreamingAggregator(const TPlanNode& tnode);
    ~SortedStreamingAggregator();

    Status prepare(RuntimeState* state, ObjectPool* pool, RuntimeProfile* runtime_profile,
                   MemTracker* mem_tracker) override;
    Status streaming_compute_agg_state(size_t chunk_size);

    StatusOr<vectorized::ChunkPtr> pull_eos_chunk();

private:
    Status _compure_group_by(size_t chunk_size);

    Status _update_states(size_t chunk_size);

    void _get_agg_result_columns(size_t chunk_size, const std::vector<uint8_t>& selector,
                                 vectorized::Columns& agg_result_columns);
    void _close_group_by(size_t chunk_size, const std::vector<uint8_t>& selector);

    Status _build_group_by_columns(size_t chunk_size, size_t selected_size, const std::vector<uint8_t>& selector,
                                   vectorized::Columns& agg_group_by_columns);

    vectorized::AggDataPtr _last_state = nullptr;
    vectorized::Columns _last_columns;
    std::vector<uint8_t> _cmp_vector;
    std::shared_ptr<StateAllocator> _streaming_state_allocator;
};
} // namespace starrocks