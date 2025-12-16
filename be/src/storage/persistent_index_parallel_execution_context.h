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

// ParallelExecutionSlot represents a working slot for one parallel task.
// Each parallel get/upsert task will have its own slot to store intermediate results,
// avoiding data races between concurrent tasks.
struct ParallelExecutionSlot {
    std::vector<Slice> keys;         // Primary keys for this task
    std::vector<uint64_t> values;    // New row IDs (rssid + segment row id) to be inserted/updated
    std::vector<uint64_t> old_values; // Existing row IDs found in the index (for get operations)
    MutableColumnPtr pk_column;      // Encoded primary key column for this batch
};

// ParallelExecutionContext manages the shared state and coordination for parallel index operations.
// It enables concurrent get/upsert operations on the persistent index during tablet publish.
//
// Thread Safety:
// - Multiple threads can execute get/upsert operations concurrently using different slots
// - The mutex protects shared state (deletes, status) when tasks complete
// - Each task gets its own slot to avoid contention during execution
struct ParallelExecutionContext {
    // Thread pool token for submitting parallel tasks. If nullptr, operations run serially.
    ThreadPoolToken* token = nullptr;

    // Mutex protecting shared state updates (deletes, status) when tasks complete
    std::mutex* mutex = nullptr;

    // Maps segment ID -> row IDs to delete. Updated when parallel get finds existing rows.
    // Protected by mutex during concurrent updates.
    std::unordered_map<uint32_t, vector<uint32_t>>* deletes = nullptr;

    // Aggregated status from all parallel tasks. Any task failure is recorded here.
    // Protected by mutex during concurrent updates.
    Status* status = nullptr;

    // Iterator over segments to process. Each task gets the next segment via current().
    lake::SegmentPKEncodeResult* segment_pk_encode_result = nullptr;

    // Working slots for parallel tasks. Each task allocates one slot via extend_slots().
    // Slots are accessed sequentially (no concurrent access to the same slot).
    std::vector<std::unique_ptr<ParallelExecutionSlot>> slots;

    // Allocates a new slot for a parallel task. Called sequentially before submitting tasks.
    void extend_slots() { slots.emplace_back(std::make_unique<ParallelExecutionSlot>()); }
};
} // namespace starrocks