// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>

#include "common/statusor.h"
#include "storage/edit_version.h"
#include "util/phmap/phmap.h"

namespace starrocks {

template <size_t KeySize>
struct FixedKey {
    uint8_t data[KeySize];
};

template <size_t KeySize>
bool operator==(const FixedKey<KeySize>& lhs, const FixedKey<KeySize>& rhs) {
    return memcmp(lhs.data, rhs.data, KeySize) == 0;
}

using IndexValue = uint64_t;
static constexpr IndexValue NullIndexValue = -1;

constexpr size_t PageSize = 4096;
constexpr size_t PageHeaderSize = 64;
constexpr size_t BucketPadding = 16;
constexpr size_t BucketPerPage = 16;

template <class T, class P>
T npad(T v, P p) {
    return (v + p - 1) / p;
}

template <class T, class P>
T pad(T v, P p) {
    return npad(v, p) * p;
}

struct IndexHash {
    IndexHash(uint64_t hash) : hash(hash) {}
    uint64_t hash;
    uint64_t shard() const { return hash >> 48; }
    uint64_t page() const { return (hash >> 16) & 0xffffffff; }
    uint64_t bucket() const { return (hash >> 8) & (BucketPerPage - 1); }
    uint64_t tag() const { return hash & 0xff; }
};

struct L1Checks {
    std::vector<uint32_t> key_idxes;
    std::vector<uint64_t> hashes;
};

class MutableIndex {
public:
    virtual ~MutableIndex() {}

    // batch get
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array for return values
    // |l1_checks|: information of keys that need to be further checked in L1
    // |num_found|: add the number of keys found in L0 to this argument
    virtual Status get(size_t n, const uint8_t* keys, IndexValue* values, L1Checks* l1_checks, size_t* num_found);

    // batch upsert
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |old_values|: return old values for updates, or set to NullValue for inserts
    // |l1_checks|: information of keys that need to be further checked in L1
    // |num_found|: add the number of keys found in L0 to this argument
    virtual Status upsert(size_t n, const uint8_t* keys, const IndexValue* values, IndexValue* old_values,
                          L1Checks* l1_checks, size_t* num_found);

    // batch insert
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    virtual Status insert(size_t n, const uint8_t* keys, const IndexValue* values);

    // batch erase(delete)
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |old_values|: return old values for updates, or set to NullValue if not exists
    // |l1_checks|: information of keys that need to be further checked in L1
    // |num_found|: add the number of keys found in L0 to this argument
    virtual Status erase(size_t n, const uint8_t* keys, IndexValue* old_values, L1Checks* l1_checks, size_t* num_found);

    static StatusOr<std::unique_ptr<MutableIndex>> create(size_t key_size);
};

class ImmutableIndex {
public:
    // batch get
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |l1_checks|: information of keys that need to be further checked in L1
    // |values|: value array for return values
    // |num_found|: add the number of keys found in L1 to this argument
    Status get(size_t n, const uint8_t* keys, const L1Checks& l1_checks, IndexValue* values, size_t* num_found);

    // batch check key existence
    Status check_not_exist(size_t n, const uint8_t* keys);
};

// A persistent primary index contains an in-memory L0 and an on-SSD/NVMe L1,
// this saves memory usage comparing to the orig all-in-memory implementation.
// This is a internal class and is intended to be used by PrimaryIndex internally.
// TODO: code skeleton currently, implementation in future PRs
//
// Currently primary index is only modified in TabletUpdates::apply process, it's
// typical use pattern in apply:
//   pi.prepare(version)
//   if (pi.upsert(upsert_keys, values, old_values))
//   pi.erase(delete_keys, old_values)
//   pi.commit()
// If any error occurred between prepare and commit, abort should be called, the
// index maybe corrupted, currently for simplicity, the whole index is cleared and rebuilt.
class PersistentIndex {
public:
    // |path|: directory that contains index files
    PersistentIndex(const std::string& path);
    ~PersistentIndex();

    // if index is loaded
    bool loaded() const { return (bool)_l0; }

    std::string path() const { return _path; }

    size_t key_size() const { return _key_size; }

    size_t size() const { return _size; }

    EditVersion version() const { return _version; }

    // create new empty index
    Status create(size_t key_size, const EditVersion& version);

    // load required states from underlying file
    Status load();

    // start modification with intended version
    Status prepare(const EditVersion& version);

    // abort modification
    Status abort();

    // commit modification
    Status commit();

    // batch index operations

    // batch get
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array for return values
    Status get(size_t n, const uint8_t* keys, IndexValue* values);

    // batch upsert
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |old_values|: return old values for updates, or set to NullValue for inserts
    Status upsert(size_t n, const uint8_t* keys, const IndexValue* values, IndexValue* old_values);

    // batch insert, return error if key already exists
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |check_l1|: also check l1 for insertion consistency(key must not exist previously), may imply heavy IO costs
    Status insert(size_t n, const uint8_t* keys, const IndexValue* values, bool check_l1);

    // batch erase
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |old_values|: return old values if key exist, or set to NullValue if not
    Status erase(size_t n, const uint8_t* keys, IndexValue* old_values);

private:
    // index storage directory
    std::string _path;
    size_t _key_size = 0;
    size_t _size = 0;
    EditVersion _version;
    std::unique_ptr<MutableIndex> _l0;
    std::unique_ptr<ImmutableIndex> _l1;
};

} // namespace starrocks
