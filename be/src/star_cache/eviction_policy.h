// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include "common/status.h"

namespace starrocks::starcache {

template <typename T>
class EvictionPolicy {
public:
    class Handle {
    public:
        Handle(EvictionPolicy* policy, void* hdl) : _policy(policy), _hdl(hdl) {}
        ~Handle() {
            if (_hdl && _policy) {
                _policy->release(_hdl);
                _hdl = nullptr;
            }
        }
    private:
        EvictionPolicy* _policy;
        void* _hdl;
    };

    using HandlePtr = std::shared_ptr<Handle>;

    virtual ~EvictionPolicy() = default;

	// Add the given id to the evict component
	virtual bool add(const T& id, size_t size) = 0;

	// Record the hit of the id
	virtual HandlePtr touch(const T& id) = 0;

	// Evict some items in current component
	virtual void evict(size_t count, std::vector<T>* evicted) = 0;

	// Evict some items in current component to store given `id`
    // This function is useful when evicting some items which are devided into
    // different buckets by hash of keys.
	virtual void evict_for(const T& id, size_t count, std::vector<T>* evicted) = 0;

    // Release the item handle returned by touch
	virtual void release(void* hdl) = 0;

	// Remove the given id from the evict component, used for `pin` function.
	virtual void remove(const T& id) = 0;

	// Clear all items in the evict component
	virtual void clear() = 0;

};

} // namespace starrocks::starcache
