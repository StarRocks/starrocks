// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/chunk.h"

namespace starrocks {
namespace pipeline {
template <typename T>
class Stack {
public:
    void reserve(size_t n) { _items.reserve(n); }

    void push(const T& p) { _items.push_back(p); }

    void push(T&& v) { _items.emplace_back(std::move(v)); }

    void clear() { _items.clear(); }

    // REQUIRES: not empty.
    T pop() {
        DCHECK(!_items.empty());
        T v = _items.back();
        _items.pop_back();
        return v;
    }

    size_t size() const { return _items.size(); }

    bool empty() const { return _items.empty(); }

    void reverse() { std::reverse(_items.begin(), _items.end()); }

private:
    std::vector<T> _items;
};

struct ChunkPoolManager {
    std::mutex mtx;
    Stack<vectorized::ChunkPtr> chunk_pool;
};

} // namespace pipeline
} // namespace starrocks
