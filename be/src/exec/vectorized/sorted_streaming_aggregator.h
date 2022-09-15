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

    Status streaming_compute_agg_state(size_t chunk_size);

    StatusOr<vectorized::ChunkPtr> pull_eos_chunk();

private:
    vectorized::AggDataPtr _last_state = nullptr;
    vectorized::Columns _last_columns;
    std::vector<uint8_t> _cmp_vector;
    std::shared_ptr<StateAllocator> _streaming_state_allocator;
};
} // namespace starrocks