// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include "star_cache/common/config.h"
#include "star_cache/common/lru_container.h"
#include "star_cache/eviction_policy.h"

namespace starrocks::starcache {

template <typename T>
class LruEvictionPolicy : public EvictionPolicy<T> {
public:
    using HandlePtr = typename EvictionPolicy<T>::HandlePtr;

    LruEvictionPolicy(size_t capacity)
        : _lru_container(new ShardedLRUContainer(1lu << config::FLAGS_lru_container_shard_bits)) {}

    ~LruEvictionPolicy() override;

	bool add(const T& id, size_t size) override;

	HandlePtr touch(const T& id) override;

	void evict(size_t count, std::vector<T>* evicted) override;

	void evict_for(const T& id, size_t count, std::vector<T>* evicted) override;

	void release(void* hdl) override;

	void remove(const T& id) override;

	void clear() override;

private:
    std::unique_ptr<ShardedLRUContainer> _lru_container = nullptr;
};

} // namespace starrocks::starcache

#include "star_cache/lru_eviction_policy-inl.h"
  
