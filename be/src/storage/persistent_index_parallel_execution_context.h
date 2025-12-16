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

#include <mutex>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "util/slice.h"

namespace starrocks {
class ThreadPoolToken;

namespace lake {
class SegmentPKEncodeResult;
}

// context's slot, per parallel execution
struct ParallelExecutionSlot {
    std::vector<Slice> keys;
    std::vector<uint64_t> values;
    std::vector<uint64_t> old_values;
    MutableColumnPtr pk_column;
};

struct ParallelExecutionContext {
    ThreadPoolToken* token = nullptr;
    std::mutex* mutex = nullptr;
    std::unordered_map<uint32_t, vector<uint32_t>>* deletes = nullptr;
    Status* status = nullptr;
    lake::SegmentPKEncodeResult* segment_pk_encode_result = nullptr;
    std::vector<std::unique_ptr<ParallelExecutionSlot>> slots;

    void extend_slots() { slots.emplace_back(std::make_unique<ParallelExecutionSlot>()); }
};
} // namespace starrocks