// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>

namespace starrocks {

template <int NUM_PRIORITY, class T, class Container>
inline bool PriorityQueue<NUM_PRIORITY, T, Container>::empty() const noexcept {
    for (const auto& q : _queues) {
        if (!q.empty()) return false;
    }
    return true;
}

template <int NUM_PRIORITY, class T, class Container>
inline typename PriorityQueue<NUM_PRIORITY, T, Container>::size_type PriorityQueue<NUM_PRIORITY, T, Container>::size()
        const noexcept {
    size_t sz = 0;
    for (const auto& q : _queues) {
        sz += q.size();
    }
    return sz;
}

template <int NUM_PRIORITY, class T, class Container>
inline void PriorityQueue<NUM_PRIORITY, T, Container>::push_back(int pri, const T& value) {
    assert(pri >= 0);
    assert(pri < NUM_PRIORITY);
    _queues[NUM_PRIORITY - pri - 1].push_back(value);
}

template <int NUM_PRIORITY, class T, class Container>
inline void PriorityQueue<NUM_PRIORITY, T, Container>::push_back(int pri, T&& value) {
    assert(pri >= 0);
    assert(pri < NUM_PRIORITY);
    _queues[NUM_PRIORITY - pri - 1].push_back(std::move(value));
}

template <int NUM_PRIORITY, class T, class Container>
template <class... Args>
inline typename PriorityQueue<NUM_PRIORITY, T, Container>::reference
PriorityQueue<NUM_PRIORITY, T, Container>::emplace_back(int pri, Args&&... args) {
    assert(pri >= 0);
    assert(pri < NUM_PRIORITY);
    return _queues[NUM_PRIORITY - pri - 1].emplace_back(std::forward<Args>(args)...);
}

template <int NUM_PRIORITY, class T, class Container>
inline void PriorityQueue<NUM_PRIORITY, T, Container>::pop_front() {
    for (auto& q : _queues) {
        if (!q.empty()) {
            q.pop_front();
            return;
        }
    }
    // undefined behavior
    _queues[0].pop_front();
}

template <int NUM_PRIORITY, class T, class Container>
inline typename PriorityQueue<NUM_PRIORITY, T, Container>::reference
PriorityQueue<NUM_PRIORITY, T, Container>::front() {
    for (auto& q : _queues) {
        if (!q.empty()) {
            return q.front();
        }
    }
    // undefined behavior
    return _queues[0].front();
}

template <int NUM_PRIORITY, class T, class Container>
inline typename PriorityQueue<NUM_PRIORITY, T, Container>::const_reference
PriorityQueue<NUM_PRIORITY, T, Container>::front() const {
    for (const auto& q : _queues) {
        if (!q.empty()) {
            return q.front();
        }
    }
    // undefined behavior
    return _queues[0].front();
}

} // namespace starrocks
