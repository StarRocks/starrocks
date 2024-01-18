// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <glog/logging.h>

#include <atomic>
#include <iostream>
#include <list>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "runtime/mem_tracker.h"
#include "storage/rowset_update_state.h"
#include "util/time.h"

namespace starrocks {

// A LRU cache implementation supports:
// * template Key type
// * atomic get_or_create
// * mutable object: memory usage of the object can change
// * mutable capacity: can change capacity at runtime
// Note: the capacity is a soft limit, it will only free unused objects
// to reduce memory usage, but if currently used(pinned) objects' memory
// exceeds capacity, that's allowed.
template <class Key, class T>
class DynamicCache {
public:
    struct Entry {
    public:
        Entry(DynamicCache<Key, T>& cache, Key key) : _cache(cache), _key(std::move(key)), _ref(1) {}

        const Key& key() const { return _key; }
        T& value() { return _value; }

        const size_t size() const { return _size; }

        void update_expire_time(int64_t expire_ms) { _expire_ms = expire_ms; }

    protected:
        friend class DynamicCache<Key, T>;
        typedef typename std::list<Entry*>::const_iterator Handle;

        DynamicCache<Key, T>& _cache;
        Handle _handle;
        Key _key;
        size_t _size = 0;
        int64_t _expire_ms = INT64_MAX;
        std::atomic<uint32_t> _ref;
        T _value;
    };

    typedef typename std::list<Entry*> List;
    typedef typename List::const_iterator Handle;
    typedef typename std::unordered_map<Key, Handle> Map;

    DynamicCache(size_t capacity) : _object_size(0), _size(0), _capacity(capacity) {}
    ~DynamicCache() {
        std::lock_guard<std::mutex> lg(_lock);
        _object_size = 0;
        _size = 0;
        auto itr = _list.begin();
        while (itr != _list.end()) {
            Entry* iobj = (*itr);
            if (iobj->_ref != 1) {
                // usually ~DynamicCache is called when BE process exists, so it's acceptable if
                // reference count is inconsistent or other thread is using this object,
                // just log an error to avoid UT failure for now.
                LOG(ERROR) << "cached entry ref=" << iobj->_ref << " key:" << iobj->_key;
            }
            delete iobj;
            itr++;
        }
        _map.clear();
        _list.clear();
    }

    void set_mem_tracker(MemTracker* mem_tracker) {
        DCHECK(_mem_tracker == nullptr);
        _mem_tracker = mem_tracker;
    }

    // get or return null
    Entry* get(const Key& key) {
        std::lock_guard<std::mutex> lg(_lock);
        auto itr = _map.find(key);
        if (itr == _map.end()) {
            return nullptr;
        }
        auto& v = itr->second;
        _list.splice(_list.end(), _list, v);
        (*v)->_ref++;
        return (*v);
    }

    // atomic get_or_create operation, to prevent loading
    // same resource multiple times
    Entry* get_or_create(const Key& key) {
        std::lock_guard<std::mutex> lg(_lock);
        auto itr = _map.find(key);
        if (itr == _map.end()) {
            // at first all created object is with size 0
            // so no need to evict now
            Entry* insert = new Entry(*this, key);
            _list.emplace_back(insert);
            Handle ret = --_list.end();
            insert->_handle = ret;
            _map[key] = ret;
            (*ret)->_ref++;
            _object_size++;
            if (insert->_size > 0) {
                _size += insert->_size;
                if (_mem_tracker) _mem_tracker->consume(insert->_size);
                _evict();
            }
            return *ret;
        } else {
            auto& v = itr->second;
            _list.splice(_list.end(), _list, v);
            (*v)->_ref++;
            return *v;
        }
    }

    // release(unuse) an object get/get_or_create'ed earlier
    void release(Entry* entry) {
        std::lock_guard<std::mutex> lg(_lock);
        // CHECK _ref > 1
        entry->_ref--;
        if (entry->_ref > 0) {
            _list.splice(_list.end(), _list, entry->_handle);
        } else {
            LOG(ERROR) << "release() got error: cache entry ref == 0 " << entry->_value << " delete";
            DCHECK(false);
            _map.erase(entry->key());
            _list.erase(entry->_handle);
            _object_size--;
            _size -= entry->_size;
            if (_mem_tracker) _mem_tracker->release(entry->_size);
            delete entry;
        }
    }

    // remove an object get/get_or_create'ed earlier
    void remove(Entry* entry) {
        std::lock_guard<std::mutex> lg(_lock);
        entry->_ref--;
        if (entry->_ref != 1) {
            LOG(ERROR) << "remove() failed: cache entry ref != 1 " << entry->_value;
            DCHECK(false);
        } else {
            _map.erase(entry->key());
            _list.erase(entry->_handle);
            _object_size--;
            _size -= entry->_size;
            if (_mem_tracker) _mem_tracker->release(entry->_size);
            delete entry;
        }
    }

    // try remove object by key
    // return true if object not exist or be removed
    // if no one use this object, object will be removed
    // otherwise do not remove the object, return false
    bool try_remove_by_key(const Key& key) {
        std::lock_guard<std::mutex> lg(_lock);
        auto itr = _map.find(key);
        if (itr == _map.end()) {
            return true;
        }
        auto v = itr->second;
        auto entry = *v;
        if (entry->_ref != 1) {
            VLOG(1) << "try_remove_by_key() failed: cache entry ref != 1 " << entry->_value;
            return false;
        } else {
            _map.erase(itr);
            _list.erase(v);
            _object_size--;
            _size -= entry->_size;
            if (_mem_tracker) _mem_tracker->release(entry->_size);
            delete entry;
        }
        return true;
    }

    // remove object by key
    // return true if object exist and is removed
    bool remove_by_key(const Key& key) {
        std::lock_guard<std::mutex> lg(_lock);
        auto itr = _map.find(key);
        if (itr == _map.end()) {
            return false;
        }
        auto v = itr->second;
        auto entry = *v;
        if (entry->_ref != 1) {
            LOG(ERROR) << "remove_by_key() failed: cache entry ref != 1 " << entry->_value;
            DCHECK(false);
        } else {
            _map.erase(itr);
            _list.erase(v);
            _object_size--;
            _size -= entry->_size;
            if (_mem_tracker) _mem_tracker->release(entry->_size);
            delete entry;
        }
        return true;
    }

    // size used by object may change, this method
    // track size changes and evict objects accordingly
    // return false if actual memory usage is larger than capacity
    bool update_object_size(Entry* entry, size_t new_size) {
        std::lock_guard<std::mutex> lg(_lock);
        _size += new_size - entry->_size;
        if (_mem_tracker) _mem_tracker->consume(new_size - entry->_size);
        entry->_size = new_size;
        return _evict();
    }

    // clear all unused *and* expired objects
    void clear_expired() {
        std::vector<Entry*> entry_list;
        {
            int64_t now = MonotonicMillis();
            std::lock_guard<std::mutex> lg(_lock);
            auto itr = _list.begin();
            while (itr != _list.end()) {
                Entry* entry = (*itr);
                if (entry->_ref == 1 && now >= entry->_expire_ms) {
                    // no usage, can remove
                    _map.erase(entry->key());
                    itr = _list.erase(itr);
                    _object_size--;
                    _size -= entry->_size;
                    if (_mem_tracker) _mem_tracker->release(entry->_size);
                    entry_list.push_back(entry);
                } else {
                    itr++;
                }
            }
        }
        for (Entry* entry : entry_list) {
            delete entry;
        }
    }

    // clear all currently unused objects
    void clear() {
        std::vector<Entry*> entry_list;
        {
            std::lock_guard<std::mutex> lg(_lock);
            auto itr = _list.begin();
            while (itr != _list.end()) {
                Entry* entry = (*itr);
                if (entry->_ref == 1) {
                    // no usage, can remove
                    _map.erase(entry->key());
                    itr = _list.erase(itr);
                    _object_size--;
                    _size -= entry->_size;
                    if (_mem_tracker) _mem_tracker->release(entry->_size);
                    entry_list.push_back(entry);
                } else {
                    itr++;
                }
            }
        }
        for (Entry* entry : entry_list) {
            delete entry;
        }
    }

    size_t object_size() const { return _object_size; }
    size_t size() const { return _size; }

    size_t capacity() const { return _capacity; }

    // adjust capacity
    // return false if actual memory usage is larger than capacity
    bool set_capacity(size_t capacity) {
        std::lock_guard<std::mutex> lg(_lock);
        _capacity = capacity;
        return _evict();
    }

    std::vector<std::pair<Key, size_t>> get_entry_sizes() const {
        std::lock_guard<std::mutex> lg(_lock);
        std::vector<std::pair<Key, size_t>> ret(_map.size());
        auto itr = _list.begin();
        while (itr != _list.end()) {
            Entry* entry = (*itr);
            ret.emplace_back(entry->key(), entry->size());
            itr++;
        }
        return ret;
    }

    void try_evict(size_t target_capacity) {
<<<<<<< HEAD
        std::lock_guard<std::mutex> lg(_lock);
        _evict(target_capacity);
        return;
    }

private:
    bool _evict(size_t target_capacity) {
=======
        std::vector<Entry*> entry_list;
        {
            std::lock_guard<std::mutex> lg(_lock);
            _evict(target_capacity, &entry_list);
        }
        for (Entry* entry : entry_list) {
            delete entry;
        }
        return;
    }

    bool TEST_evict(size_t target_capacity, std::vector<Entry*>* entry_list) {
        return _evict(target_capacity, entry_list);
    }

private:
    bool _evict(size_t target_capacity, std::vector<Entry*>* entry_list) {
>>>>>>> 2.5.18
        auto itr = _list.begin();
        while (_size > target_capacity && itr != _list.end()) {
            Entry* entry = (*itr);
            // no need to check iobj != obj, cause obj is in use, so _ref > 1
            if (entry->_ref == 1) {
                // no usage, can remove
                _map.erase(entry->key());
                itr = _list.erase(itr);
                _object_size--;
                _size -= entry->_size;
                if (_mem_tracker) _mem_tracker->release(entry->_size);
                entry_list->push_back(entry);
            } else {
                itr++;
            }
        }
        return _size <= _capacity;
    }

<<<<<<< HEAD
    bool _evict() { return _evict(_capacity); }
=======
    bool _evict() {
        std::vector<Entry*> entry_list;
        bool ret = _evict(_capacity, &entry_list);
        for (Entry* entry : entry_list) {
            delete entry;
        }
        return ret;
    }
>>>>>>> 2.5.18

    mutable std::mutex _lock;
    List _list;
    Map _map;
    size_t _object_size;
    std::atomic<size_t> _size;
    size_t _capacity = 0;

    MemTracker* _mem_tracker = nullptr;
};

} // namespace starrocks
