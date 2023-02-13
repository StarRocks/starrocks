// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#pragma once

#include <rapidjson/document.h>

#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace starrocks::starcache {

class LRUKey {
public:
    LRUKey() {}
    // Create a slice that refers to d[0,n-1].
    LRUKey(const char* d, size_t n) : _data(d), _size(n) {}

    // Create a slice that refers to the contents of "s"
    LRUKey(const std::string& s) : _data(s.data()), _size(s.size()) {}

    // Create a slice that refers to the contents of s[0,strlen(s)-1]
    LRUKey(const char* s) : _data(s), _size(strlen(s)) {}

    // Create a slice that refers to the contents of "s"
    LRUKey(std::string_view s) : _data(s.data()), _size(s.size()) {}

    ~LRUKey() = default;

    // Return a pointer to the beginning of the referenced data
    const char* data() const { return _data; }

    // Return the length (in bytes) of the referenced data
    size_t size() const { return _size; }

    // Return true iff the length of the referenced data is zero
    bool empty() const { return _size == 0; }

    // Return the ith byte in the referenced data.
    // REQUIRES: n < size()
    char operator[](size_t n) const {
        assert(n < size());
        return _data[n];
    }

    // Change this slice to refer to an empty array
    void clear() {
        _data = nullptr;
        _size = 0;
    }

    // Drop the first "n" bytes from this slice.
    void remove_prefix(size_t n) {
        assert(n <= size());
        _data += n;
        _size -= n;
    }

    // Return a string that contains the copy of the referenced data.
    std::string to_string() const { return std::string(_data, _size); }

    bool operator==(const LRUKey& other) const {
        return ((size() == other.size()) && (memcmp(data(), other.data(), size()) == 0));
    }

    bool operator!=(const LRUKey& other) const { return !(*this == other); }

    int compare(const LRUKey& b) const {
        const size_t min_len = (_size < b._size) ? _size : b._size;
        int r = memcmp(_data, b._data, min_len);
        if (r == 0) {
            if (_size < b._size) {
                r = -1;
            } else if (_size > b._size) {
                r = +1;
            }
        }
        return r;
    }

    uint32_t hash(const char* data, size_t n, uint32_t seed) const;

    // Return true iff "x" is a prefix of "*this"
    bool starts_with(const LRUKey& x) const { return ((_size >= x._size) && (memcmp(_data, x._data, x._size) == 0)); }

private:
    uint32_t _decode_fixed32(const char* ptr) const {
        // Load the raw bytes
        uint32_t result;
        memcpy(&result, ptr, sizeof(result)); // gcc optimizes this to a plain load
        return result;
    }

    const char* _data{nullptr};
    size_t _size{0};
};

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
typedef struct LRUHandle {
    void* value;
    void (*deleter)(const LRUKey&, void* value);
    LRUHandle* next_hash;
    LRUHandle* next;
    LRUHandle* prev;
    size_t charge;
    size_t key_length;
    // Whether entry is in the container.
    bool valid;
    uint32_t refs;
    // Hash of key(); used for fast sharding and comparisons
    uint32_t hash;
    // Beginning of key
    char key_data[1];

    LRUKey key() const {
        // For cheaper lookups, we allow a temporary Handle object
        // to store a pointer to a key in "value".
        if (next == this) {
            return *(reinterpret_cast<LRUKey*>(value));
        } else {
            return LRUKey(key_data, key_length);
        }
    }

    void free() {
        (*deleter)(key(), value);
        ::free(this);
    }

} LRUHandle;

// We provide our own simple hash tablet since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// tablet implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.

class HandleTable {
public:
    HandleTable() { _resize(); }

    ~HandleTable() { delete[] _list; }

    LRUHandle* lookup(const LRUKey& key, uint32_t hash);

    LRUHandle* insert(LRUHandle* h);

    LRUHandle* remove(const LRUKey& key, uint32_t hash);

private:
    // The tablet consists of an array of buckets where each bucket is
    // a linked list of container entries that hash into the bucket.
    uint32_t _length{0};
    uint32_t _elems{0};
    LRUHandle** _list{nullptr};

    // Return a pointer to slot that points to a container entry that
    // matches key/hash.  If there is no such container entry, return a
    // pointer to the trailing slot in the corresponding linked list.
    LRUHandle** _find_pointer(const LRUKey& key, uint32_t hash);
    bool _resize();
};

// A single shard of sharded container.
class LRUContainer {
public:
    LRUContainer();
    ~LRUContainer();

    // Like ShardedLRUContainer methods, but with an extra "hash" parameter.
    LRUHandle* insert(const LRUKey& key, uint32_t hash, size_t charge, void* value,
                   void (*deleter)(const LRUKey& key, void* value));

    LRUHandle* lookup(const LRUKey& key, uint32_t hash);

    void release(LRUHandle* handle);

    void erase(const LRUKey& key, uint32_t hash);


    void evict(size_t charge, std::vector<LRUHandle*>* evicted);
    int prune();

    uint64_t get_lookup_count();
    size_t get_usage();

private:
    void _lru_remove(LRUHandle* e);
    void _lru_append(LRUHandle* list, LRUHandle* e);
    bool _unref(LRUHandle* e);
    void _evict_from_lru(size_t charge, std::vector<LRUHandle*>* evicted);
    void _evict_one_entry(LRUHandle* e);

    // _mutex protects the following state.
    std::mutex _mutex;
    size_t _usage{0};
    uint64_t _lookup_count{0};

    // Dummy head of LRU list.
    // lru.prev is newest entry, lru.next is oldest entry.
    // Entries have refs==1 and valid==true.
    LRUHandle _lru;

    HandleTable _table;
};

//static const int kNumShardBits = 5;
static const int kNumShardBits = 1;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUContainer {
public:
    ShardedLRUContainer(size_t shard_count) : _shards(shard_count) {}

    ~ShardedLRUContainer() = default;

    // Insert a lru key with it's value into the container and assign it
    // the specified charge.
    //
    // Returns a handle that corresponds to the key.  The caller
    // must call this->release(handle) when the returned handle is no
    // longer needed.
    //
    // When the inserted entry is no longer needed, the key and
    // value will be passed to "deleter".
    LRUHandle* insert(const LRUKey& key, size_t charge, void* value, void (*deleter)(const LRUKey& key, void* value));

    // If the container has no entry for "key", returns NULL.
    //
    // Else return a handle that corresponds to the entry. The caller
    // must call this->release(handle) when the returned entry is no
    // longer needed.
    LRUHandle* lookup(const LRUKey& key);

    // Release an entry returned by a previous Lookup().
    // REQUIRES: handle must not have been released yet.
    // REQUIRES: handle must have been returned by a method on *this.
    void release(LRUHandle* handle);

    // If the container contains entry for key, erase it.  Note that the
    // underlying entry will be kept around until all existing handles
    // to it have been released.
    void erase(const LRUKey& key);

    // Remove all entries that are not actively in use. Memory-constrained
    // applications may wish to call this method to reduce memory usage.
    // A future release of leveldb may change prune() to a pure abstract method.
    void prune();

    // Used to evict elements from lru container
    void evict(size_t charge, std::vector<LRUHandle*>* evicted);

    // Used to evict elements from lru container to store the new entry, it will
    // evict the entries in the same shard with given `key`.
    void evict_for(const LRUKey& key, size_t charge, std::vector<LRUHandle*>* evicted);

    // State tracing
    void get_status(rapidjson::Document* document);
    size_t get_usage();
    uint64_t get_lookup_count();

private:
    static uint32_t _hash_slice(const LRUKey& s);
    static uint32_t _shard(uint32_t hash);
    size_t _get_stat(size_t (LRUContainer::*mem_fun)());

    std::vector<LRUContainer> _shards;
    std::mutex _mutex;
};

} // namespace starrocks::starcache
