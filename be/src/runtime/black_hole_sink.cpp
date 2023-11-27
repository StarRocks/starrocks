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

#include "runtime/black_hole_sink.h"

namespace starrocks {

BlackHoleSink::BlackHoleSink(BufferControlBlock* sinker, RuntimeProfile* parent_profile)
        : _sinker(sinker), _parent_profile(parent_profile) {}

Status BlackHoleSink::init(RuntimeState* state) {
    _eat_rows_counter = ADD_COUNTER(_parent_profile, "BlackHoleEatenRows", TUnit::UNIT);
    return Status::OK();
}

Status BlackHoleSink::append_chunk(Chunk* chunk) {
    return Status::NotSupported("BlackHole sink is not support in none-pipeline engine");
}

StatusOr<TFetchDataResultPtrs> BlackHoleSink::process_chunk(Chunk* chunk) {
    _eaten_rows_num += chunk->num_rows();
    TFetchDataResultPtrs results{};
    std::unique_ptr<TFetchDataResult> result(new (std::nothrow) TFetchDataResult());
    if (!result) {
        return Status::MemoryAllocFailed("memory allocate failed");
    }
    // Return empty result
    result->result_batch.rows.resize(0);
    results.emplace_back(std::move(result));
    return results;
}

StatusOr<bool> BlackHoleSink::try_add_batch(TFetchDataResultPtrs& results) {
    auto status = _sinker->try_add_batch(results);
    results.clear();
    if (!status.ok()) {
        LOG(WARNING) << "Append result to sink failed.";
    }
    return status;
}

Status BlackHoleSink::close() {
    COUNTER_SET(_eat_rows_counter, _eaten_rows_num);
    return Status::OK();
}

} // namespace starrocks