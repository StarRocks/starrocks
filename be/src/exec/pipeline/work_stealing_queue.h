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

#include <vector>

#include "base/concurrency/moodycamel/concurrentqueue.h"
#include "common/logging.h"

namespace starrocks::pipeline {

// WorkStealingQueue is a multi-level lock-free queue built on top of
// moodycamel::ConcurrentQueue. Each level is an independent ConcurrentQueue.
// Worker threads get pre-allocated ProducerTokens for reduced enqueue
// contention; external threads use the implicit producer path.
//
// This is the foundational building block for LockFreeDriverQueue and
// LockFreeScanTaskQueue.
template <typename T, int NUM_LEVELS>
class WorkStealingQueue {
public:
    using QueueType = moodycamel::ConcurrentQueue<T>;
    using ProducerToken = typename QueueType::producer_token_t;

    explicit WorkStealingQueue(int num_workers) : _num_workers(num_workers) {
        DCHECK(num_workers > 0) << "num_workers must be positive";

        // Pre-allocate producer tokens for each (worker, level) pair.
        // Stored flat: _producer_tokens[worker_id * NUM_LEVELS + level]
        _producer_tokens.reserve(num_workers * NUM_LEVELS);
        for (int w = 0; w < num_workers; ++w) {
            for (int l = 0; l < NUM_LEVELS; ++l) {
                _producer_tokens.emplace_back(_levels[l].queue);
            }
        }
    }

    ~WorkStealingQueue() = default;

    // Non-copyable, non-movable (ProducerTokens hold references to queues).
    WorkStealingQueue(const WorkStealingQueue&) = delete;
    WorkStealingQueue& operator=(const WorkStealingQueue&) = delete;
    WorkStealingQueue(WorkStealingQueue&&) = delete;
    WorkStealingQueue& operator=(WorkStealingQueue&&) = delete;

    // Enqueue with explicit ProducerToken (for worker threads).
    void enqueue(T item, int level, int worker_id) {
        DCHECK(level >= 0 && level < NUM_LEVELS) << "level out of range: " << level;
        DCHECK(worker_id >= 0 && worker_id < _num_workers) << "worker_id out of range: " << worker_id;

        auto& token = _producer_tokens[worker_id * NUM_LEVELS + level];
        _levels[level].queue.enqueue(token, std::move(item));
    }

    // Enqueue with implicit producer (for external threads).
    void enqueue(T item, int level) {
        DCHECK(level >= 0 && level < NUM_LEVELS) << "level out of range: " << level;

        _levels[level].queue.enqueue(std::move(item));
    }

    // Dequeue from specified level. Returns true if an item was dequeued.
    // Uses moodycamel's default heuristic (picks the fullest producer sub-queue).
    bool try_dequeue(int level, T& item) {
        DCHECK(level >= 0 && level < NUM_LEVELS) << "level out of range: " << level;

        return _levels[level].queue.try_dequeue(item);
    }

    // Returns true if the specified level appears empty.
    // Note: this is an approximation in a concurrent setting.
    bool empty(int level) const {
        DCHECK(level >= 0 && level < NUM_LEVELS) << "level out of range: " << level;

        return _levels[level].queue.size_approx() == 0;
    }

    // Returns the approximate size of the specified level.
    size_t size_approx(int level) const {
        DCHECK(level >= 0 && level < NUM_LEVELS) << "level out of range: " << level;

        return _levels[level].queue.size_approx();
    }

    // Returns the approximate total size across all levels.
    size_t size_approx() const {
        size_t total = 0;
        for (int l = 0; l < NUM_LEVELS; ++l) {
            total += _levels[l].queue.size_approx();
        }
        return total;
    }

private:
    // Each LevelQueue is cache-line aligned to prevent false sharing
    // between levels that may be accessed by different threads.
    struct alignas(64) LevelQueue {
        QueueType queue;
    };

    int _num_workers;
    LevelQueue _levels[NUM_LEVELS];
    std::vector<ProducerToken> _producer_tokens;
};

} // namespace starrocks::pipeline
