// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include "util/lru_cache.h"
#include "star_cache/eviction_policy.h"

namespace starrocks {

template <typename T>
class LruEvictionPolicy : public EvictionPolicy<T> {
public:
    LruEvictionPolicy(size_t capacity) : _lru_cache(new_lru_cache(capacity)) {}

    ~LruEvictionPolicy() override;

	bool add(const T& id) override;

	bool touch(const T& id) override;

	void evict(size_t count, std::vector<T>* evicted) override;

	void evict_for(const T& id, size_t count, std::vector<T>* evicted) override;

	void remove(const T& id) override;

	void clear() override;

private:
    std::unique_ptr<Cache> _lru_cache = nullptr;
};

} // namespace starrocks

#include "star_cache/lru_eviction_policy-inl.h"
  
