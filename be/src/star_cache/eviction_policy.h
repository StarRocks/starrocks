// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include "common/status.h"

namespace starrocks {

template <typename T>
class EvictionPolicy {
public:
    virtual ~EvictionPolicy() = default;

	// Add the given id to the evict component
	virtual bool add(const T& id) = 0;

	// Record the hit of the id
	virtual bool touch(const T& id) = 0;

	// Evict some items in current component
	virtual void evict(size_t count, std::vector<T>* evicted) = 0;

	// Evict some items in current component to store given `id`
    // This function is useful when evicting some items which are devided into
    // different buckets by hash of keys.
	virtual void evict_for(const T& id, size_t count, std::vector<T>* evicted) = 0;

	// Remove the given id from the evict component, used for `pin` function.
	virtual void remove(const T& id) = 0;

	// Clear all items in the evict component
	virtual void clear() = 0;

};

} // namespace starrocks
