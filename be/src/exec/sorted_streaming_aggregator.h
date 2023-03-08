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

#include <memory>

#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/aggregator.h"

namespace starrocks {
struct StateAllocator;
class SortedStreamingAggregator final : public Aggregator {
public:
    SortedStreamingAggregator(AggregatorParamsPtr params);
    ~SortedStreamingAggregator() override;

    Status prepare(RuntimeState* state, ObjectPool* pool, RuntimeProfile* runtime_profile) override;

    StatusOr<ChunkPtr> streaming_compute_agg_state(size_t chunk_size, bool is_update_phase = false);

    StatusOr<ChunkPtr> pull_eos_chunk();

private:
    Status _compute_group_by(size_t chunk_size);

    Status _update_states(size_t chunk_size, bool is_update_phase);

    Status _get_agg_result_columns(size_t chunk_size, const std::vector<uint8_t>& selector,
                                   Columns& agg_result_columns);
    void _close_group_by(size_t chunk_size, const std::vector<uint8_t>& selector);

    Status _build_group_by_columns(size_t chunk_size, size_t selected_size, const std::vector<uint8_t>& selector,
                                   Columns& agg_group_by_columns);

    AggDataPtr _last_state = nullptr;
    Columns _last_columns;
    std::vector<uint8_t> _cmp_vector;
    std::shared_ptr<StateAllocator> _streaming_state_allocator;
};
} // namespace starrocks
