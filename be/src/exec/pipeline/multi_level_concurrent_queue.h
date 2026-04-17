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

// MultiLevelConcurrentQueue is a multi-level concurrent queue built on top of
// moodycamel::ConcurrentQueue. Each level is an independent ConcurrentQueue.
// Worker threads get pre-allocated ConsumerTokens for reduced dequeue
// contention; all producers use the implicit producer path.
//
// ConsumerToken is critical for scalability: without it, all consumers
// converge on the same "fullest" producer sub-queue (thundering herd).
// With ConsumerToken, each consumer round-robins across producers,
// distributing dequeue load evenly.
template <typename T, int NUM_LEVELS>
class MultiLevelConcurrentQueue {
public:
    using QueueType = moodycamel::ConcurrentQueue<T>;
    using ConsumerToken = typename QueueType::consumer_token_t;

    explicit MultiLevelConcurrentQueue(int num_workers) : _num_workers(num_workers) {
        DCHECK(num_workers > 0) << "num_workers must be positive";

        // Pre-allocate consumer tokens: _consumer_tokens[worker_id * NUM_LEVELS + level]
        _consumer_tokens.reserve(num_workers * NUM_LEVELS);
        for (int w = 0; w < num_workers; ++w) {
            for (int l = 0; l < NUM_LEVELS; ++l) {
                _consumer_tokens.emplace_back(_levels[l].queue);
            }
        }
    }

    ~MultiLevelConcurrentQueue() = default;

    // Non-copyable, non-movable (tokens hold references to internal queues).
    MultiLevelConcurrentQueue(const MultiLevelConcurrentQueue&) = delete;
    MultiLevelConcurrentQueue& operator=(const MultiLevelConcurrentQueue&) = delete;
    MultiLevelConcurrentQueue(MultiLevelConcurrentQueue&&) = delete;
    MultiLevelConcurrentQueue& operator=(MultiLevelConcurrentQueue&&) = delete;

    // Enqueue from worker threads. Uses implicit producer path.
    // Returns false if the underlying queue fails to allocate storage.
    bool enqueue(T item, int level, int worker_id) {
        DCHECK(level >= 0 && level < NUM_LEVELS);
        DCHECK(worker_id >= 0 && worker_id < _num_workers);
        return _levels[level].queue.enqueue(std::move(item));
    }

    // Enqueue with implicit producer (for external threads).
    // Returns false if the underlying queue fails to allocate storage.
    bool enqueue(T item, int level) {
        DCHECK(level >= 0 && level < NUM_LEVELS);
        return _levels[level].queue.enqueue(std::move(item));
    }

    // Dequeue with ConsumerToken (for worker threads - avoids thundering herd).
    // Each worker's ConsumerToken round-robins across producer sub-queues,
    // distributing dequeue load evenly instead of all competing for the fullest.
    bool try_dequeue(int level, T& item, int worker_id) {
        DCHECK(level >= 0 && level < NUM_LEVELS);
        DCHECK(worker_id >= 0 && worker_id < _num_workers);
        return _levels[level].queue.try_dequeue(_consumer_tokens[worker_id * NUM_LEVELS + level], item);
    }

    // Dequeue without token (for external threads or when worker_id unavailable).
    // Uses heuristic: picks the fullest producer sub-queue.
    // Under high concurrency this causes thundering herd - prefer the token version.
    bool try_dequeue(int level, T& item) {
        DCHECK(level >= 0 && level < NUM_LEVELS);
        return _levels[level].queue.try_dequeue(item);
    }

    // Approximate emptiness check backed by ConcurrentQueue::size_approx().
    // Callers must not use this as an exact drain/termination condition while
    // producers or consumers are still active.
    bool empty_approx(int level) const {
        DCHECK(level >= 0 && level < NUM_LEVELS);
        return _levels[level].queue.size_approx() == 0;
    }

    size_t size_approx(int level) const {
        DCHECK(level >= 0 && level < NUM_LEVELS);
        return _levels[level].queue.size_approx();
    }

    size_t size_approx() const {
        size_t total = 0;
        for (int l = 0; l < NUM_LEVELS; ++l) {
            total += _levels[l].queue.size_approx();
        }
        return total;
    }

private:
    struct alignas(64) LevelQueue {
        QueueType queue;
    };

    int _num_workers;
    LevelQueue _levels[NUM_LEVELS];
    std::vector<ConsumerToken> _consumer_tokens; // flat: [worker_id * NUM_LEVELS + level]
};

} // namespace starrocks::pipeline
